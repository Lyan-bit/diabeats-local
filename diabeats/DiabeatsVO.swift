
import Foundation

class DiabeatsVO : Identifiable, Decodable, Encodable {

  var id: String = ""
  var pregnancies: Int = 0
  var glucose: Int = 0
  var bloodPressure: Int = 0
  var skinThickness: Int = 0
  var insulin: Int = 0
  var bmi: Double = 0.0
  var diabetesPedigreeFunction: Double = 0.0
  var age: Int = 0
  var outcome: String = ""

  static var defaultInstance : DiabeatsVO? = nil
  var errorList : [String] = [String]()

  init() {
  	//init
  }

  static func defaultDiabeatsVO() -> DiabeatsVO
  { if defaultInstance == nil
    { defaultInstance = DiabeatsVO() }
    return defaultInstance!
  }

  init(idx: String, pregnanciesx: Int, glucosex: Int, bloodPressurex: Int, skinThicknessx: Int, insulinx: Int, bmix: Double, diabetesPedigreeFunctionx: Double, agex: Int, outcomex: String)  {
    id = idx
    pregnancies = pregnanciesx
    glucose = glucosex
    bloodPressure = bloodPressurex
    skinThickness = skinThicknessx
    insulin = insulinx
    bmi = bmix
    diabetesPedigreeFunction = diabetesPedigreeFunctionx
    age = agex
    outcome = outcomex
  }

  init(x : Diabeats)  {
    id = x.id
    pregnancies = x.pregnancies
    glucose = x.glucose
    bloodPressure = x.bloodPressure
    skinThickness = x.skinThickness
    insulin = x.insulin
    bmi = x.bmi
    diabetesPedigreeFunction = x.diabetesPedigreeFunction
    age = x.age
    outcome = x.outcome
  }

  func toString() -> String
  { return " id= \(id), pregnancies= \(pregnancies), glucose= \(glucose), bloodPressure= \(bloodPressure), skinThickness= \(skinThickness), insulin= \(insulin), bmi= \(bmi), diabetesPedigreeFunction= \(diabetesPedigreeFunction), age= \(age), outcome= \(outcome) "
  }

  func getId() -> String
	  { return id }
	
  func setId(x : String)
	  { id = x }
	  
  func getPregnancies() -> Int
	  { return pregnancies }
	
  func setPregnancies(x : Int)
	  { pregnancies = x }
	  
  func getGlucose() -> Int
	  { return glucose }
	
  func setGlucose(x : Int)
	  { glucose = x }
	  
  func getBloodPressure() -> Int
	  { return bloodPressure }
	
  func setBloodPressure(x : Int)
	  { bloodPressure = x }
	  
  func getSkinThickness() -> Int
	  { return skinThickness }
	
  func setSkinThickness(x : Int)
	  { skinThickness = x }
	  
  func getInsulin() -> Int
	  { return insulin }
	
  func setInsulin(x : Int)
	  { insulin = x }
	  
  func getBmi() -> Double
	  { return bmi }
	
  func setBmi(x : Double)
	  { bmi = x }
	  
  func getDiabetesPedigreeFunction() -> Double
	  { return diabetesPedigreeFunction }
	
  func setDiabetesPedigreeFunction(x : Double)
	  { diabetesPedigreeFunction = x }
	  
  func getAge() -> Int
	  { return age }
	
  func setAge(x : Int)
	  { age = x }
	  
  func getOutcome() -> String
	  { return outcome }
	
  func setOutcome(x : String)
	  { outcome = x }

}
