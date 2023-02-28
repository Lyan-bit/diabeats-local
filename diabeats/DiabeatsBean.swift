
import Foundation

class DiabeatsBean {
	
  var errorList : [String] = [String]()

  init() {
  	 //init
  }

  func resetData() { 
  	errorList = [String]()
  }

  func isCreateDiabeatsError(id: String, pregnancies: Int, glucose: Int, bloodPressure: Int, skinThickness: Int, insulin: Int, bmi: Double, diabetesPedigreeFunction: Double, age: Int, outcome: String) -> Bool { 
  	resetData() 
  	if id == "" {
  		errorList.append("id cannot be empty")
  	}
  	if pregnancies != 0 {
	  		errorList.append("pregnancies cannot be zero")
	  	}
  	if glucose != 0 {
	  		errorList.append("glucose cannot be zero")
	  	}
  	if bloodPressure != 0 {
	  		errorList.append("bloodPressure cannot be zero")
	  	}
  	if skinThickness != 0 {
	  		errorList.append("skinThickness cannot be zero")
	  	}
  	if insulin != 0 {
	  		errorList.append("insulin cannot be zero")
	  	}
  	if bmi != 0 {
	  		errorList.append("bmi cannot be zero")
	  	}
  	if diabetesPedigreeFunction != 0 {
	  		errorList.append("diabetesPedigreeFunction cannot be zero")
	  	}
  	if age != 0 {
	  		errorList.append("age cannot be zero")
	  	}
  	if outcome == "" {
  		errorList.append("outcome cannot be empty")
  	}

    return errorList.count > 0
  }

  func isEditDiabeatsError() -> Bool
    { return false }
          
  func isListDiabeatsError() -> Bool {
    resetData() 
    return false
  }
  
   func isDeleteDiabeatserror() -> Bool
     { return false }

  func errors() -> String {
    var res : String = ""
    for (_,x) in errorList.enumerated()
    { res = res + x + ", " }
    return res
  }

}
