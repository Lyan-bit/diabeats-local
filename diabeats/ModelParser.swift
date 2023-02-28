import Foundation
import TensorFlowLite

typealias FileInfo = (name: String, extension: String)

enum ModelFile {
  static let modelInfo: FileInfo = (name: "diabeats", extension: "tflite")
}

class ModelParser {

    private var interpreter: Interpreter
    private var labels: [String] = ["positive", "negative"]
    
    init?(modelFileInfo: FileInfo, threadCount: Int = 1) {
        let modelFilename = modelFileInfo.name

        guard let modelPath = Bundle.main.path(
          forResource: modelFilename,
          ofType: modelFileInfo.extension
        ) else {
          print("Failed to load the model file")
          return nil
        }
        do {
            interpreter = try Interpreter(modelPath: modelPath)
        } catch _
        {
            print("Failed to create the interpreter")
            return nil
        }
    }
    
    func runModel(
				 input0 item0: Float, 
				 input1 item1: Float, 
				 input2 item2: Float, 
				 input3 item3: Float, 
				 input4 item4: Float, 
				 input5 item5: Float, 
				 input6 item6: Float, 
				 input7 item7: Float
				 ) -> String? {
        do {
            try interpreter.allocateTensors()
            var data = [Float] (arrayLiteral: 
				 			   item0, 
				 			   item1, 
				 			   item2, 
				 			   item3, 
				 			   item4, 
				 			   item5, 
				 			   item6, 
				 			   item7
				 			   )
            let buffer: UnsafeMutableBufferPointer<Float> = UnsafeMutableBufferPointer(start: &data, count: 8)
            try interpreter.copy(Data(buffer: buffer), toInputAt: 0)
            try interpreter.invoke()
            let outputTensor = try interpreter.output(at: 0)
            let results: [Float32] =
            [Float32](unsafeData: outputTensor.data) ?? []
            
           let result : [Inference] = getTopN(results: results, labels: labels)
           
           return "(\(result[0].label) " + String(format: "%.2f", result[0].confidence * 100) + "%)"
        }
        catch {
              print(error)
              return nil
        }
    }
}

/// An inference from invoking the `Interpreter`.
struct Inference {
  let confidence: Float
  let label: String
}

/// Returns the top N inference results sorted in descending order.
private func getTopN(results: [Float], labels: [String]) -> [Inference] {
  // Create a zipped array of tuples [(labelIndex: Int, confidence: Float)].
  let zippedResults = zip(labels.indices, results)
  
  // Sort the zipped results by confidence value in descending order.
  let sortedResults = zippedResults.sorted { $0.1 > $1.1 }.prefix(1)
  
  // Return the `Inference` results.
  return sortedResults.map { result in Inference(confidence: result.1, label: labels[result.0]) }
}
    
extension Array {
  init?(unsafeData: Data) {
    guard unsafeData.count % MemoryLayout<Element>.stride == 0
          else { return nil }
    #if swift(>=5.0)
    self = unsafeData.withUnsafeBytes {
      .init($0.bindMemory(to: Element.self))
    }
    #else
    self = unsafeData.withUnsafeBytes {
      .init(UnsafeBufferPointer<Element>(
        start: $0,
        count: unsafeData.count / MemoryLayout<Element>.stride
      ))
    }
    #endif  // swift(>=5.0)
  }
}
