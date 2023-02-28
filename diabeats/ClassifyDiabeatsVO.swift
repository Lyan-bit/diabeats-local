
import Foundation

class ClassifyDiabeatsVO {
  var diabeats : String = ""

  static var defaultInstance : ClassifyDiabeatsVO? = nil
  var errorList : [String] = [String]()

  var result : String = ""

  init() {
  	//init
  }
  
  static func defaultClassifyDiabeatsVO() -> ClassifyDiabeatsVO
  { if defaultInstance == nil
    { defaultInstance = ClassifyDiabeatsVO() }
    return defaultInstance!
  }

  init(diabeatsx: String)  {
  diabeats = diabeatsx
  }

  func toString() -> String
  	{ return "" + "diabeats = " + diabeats }

  func getDiabeats() -> Diabeats
  	{ return Diabeats.diabeatsIndex[diabeats]! }
  	
  func setDiabeats(x : Diabeats)
  	{ diabeats = x.id }
			  
  func setResult (x: String) {
    result = x }
          
  func resetData()
  	{ errorList = [String]() }

  func isClassifyDiabeatsError() -> Bool
  	{ resetData()
  
 if Diabeats.diabeatsIndex[diabeats] == nil
	{ errorList.append("Invalid diabeats id: " + diabeats) }


    if errorList.count > 0
    { return true }
    
    return false
  }

  func errors() -> String
  { var res : String = ""
    for (_,x) in errorList.enumerated()
    { res = res + x + ", " }
    return res
  }

}

