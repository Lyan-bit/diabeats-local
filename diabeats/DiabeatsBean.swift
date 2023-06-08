
import Foundation

class DiabeatsBean {
	
  var errorList : [String] = [String]()

  init() {
  	 //init
  }

  func resetData() { 
  	errorList = [String]()
  }

  func isCreateDiabeatsError(diabeats: DiabeatsVO) -> Bool { 
  	resetData() 
  	if diabeats.id == "" {
  		errorList.append("id cannot be empty")
  	}
  	if diabeats.pregnancies != 0 {
	  		errorList.append("pregnancies cannot be zero")
	  	}
  	if diabeats.glucose != 0 {
	  		errorList.append("glucose cannot be zero")
	  	}
  	if diabeats.bloodPressure != 0 {
	  		errorList.append("bloodPressure cannot be zero")
	  	}
  	if diabeats.skinThickness != 0 {
	  		errorList.append("skinThickness cannot be zero")
	  	}
  	if diabeats.insulin != 0 {
	  		errorList.append("insulin cannot be zero")
	  	}
  	if diabeats.bmi != 0 {
	  		errorList.append("bmi cannot be zero")
	  	}
  	if diabeats.diabetesPedigreeFunction != 0 {
	  		errorList.append("diabetesPedigreeFunction cannot be zero")
	  	}
  	if diabeats.age != 0 {
	  		errorList.append("age cannot be zero")
	  	}
  	if diabeats.outcome == "" {
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
