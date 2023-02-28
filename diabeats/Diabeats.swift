
import Foundation

class Diabeats  {
	
  private static var instance : Diabeats? = nil	
  
  init() { 
  	//init
  }
  
  init(copyFrom: Diabeats) {
  	self.id = "copy" + copyFrom.id
  	self.pregnancies = copyFrom.pregnancies
  	self.glucose = copyFrom.glucose
  	self.bloodPressure = copyFrom.bloodPressure
  	self.skinThickness = copyFrom.skinThickness
  	self.insulin = copyFrom.insulin
  	self.bmi = copyFrom.bmi
  	self.diabetesPedigreeFunction = copyFrom.diabetesPedigreeFunction
  	self.age = copyFrom.age
  	self.outcome = copyFrom.outcome
  }
  
  func copy() -> Diabeats
  { let res : Diabeats = Diabeats(copyFrom: self)
  	addDiabeats(instance: res)
  	return res
  }
  
static func defaultInstanceDiabeats() -> Diabeats
    { if (instance == nil)
    { instance = createDiabeats() }
    return instance!
}

deinit
{ killDiabeats(obj: self) }	


  var id: String = ""  /* primary key */
  var pregnancies: Int = 0 
  var glucose: Int = 0 
  var bloodPressure: Int = 0 
  var skinThickness: Int = 0 
  var insulin: Int = 0 
  var bmi: Double = 0.0 
  var diabetesPedigreeFunction: Double = 0.0 
  var age: Int = 0 
  var outcome: String = "" 

  static var diabeatsIndex : Dictionary<String,Diabeats> = [String:Diabeats]()

  static func getByPKDiabeats(index : String) -> Diabeats?
  { return diabeatsIndex[index] }


}

  var DiabeatsAllInstances : [Diabeats] = [Diabeats]()

  func createDiabeats() -> Diabeats
	{ let result : Diabeats = Diabeats()
	  DiabeatsAllInstances.append(result)
	  return result }
  
  func addDiabeats(instance : Diabeats)
	{ DiabeatsAllInstances.append(instance) }

  func killDiabeats(obj: Diabeats)
	{ DiabeatsAllInstances = DiabeatsAllInstances.filter{ $0 !== obj } }

  func createByPKDiabeats(key : String) -> Diabeats
	{ var result : Diabeats? = Diabeats.getByPKDiabeats(index: key)
	  if result != nil { 
	  	return result!
	  }
	  result = Diabeats()
	  DiabeatsAllInstances.append(result!)
	  Diabeats.diabeatsIndex[key] = result!
	  result!.id = key
	  return result! }

  func killDiabeats(key : String)
	{ Diabeats.diabeatsIndex[key] = nil
	  DiabeatsAllInstances.removeAll(where: { $0.id == key })
	}
	
	extension Diabeats : Hashable, Identifiable
	{ 
	  static func == (lhs: Diabeats, rhs: Diabeats) -> Bool
	  {       lhs.id == rhs.id &&
      lhs.pregnancies == rhs.pregnancies &&
      lhs.glucose == rhs.glucose &&
      lhs.bloodPressure == rhs.bloodPressure &&
      lhs.skinThickness == rhs.skinThickness &&
      lhs.insulin == rhs.insulin &&
      lhs.bmi == rhs.bmi &&
      lhs.diabetesPedigreeFunction == rhs.diabetesPedigreeFunction &&
      lhs.age == rhs.age &&
      lhs.outcome == rhs.outcome
	  }
	
	  func hash(into hasher: inout Hasher) {
    	hasher.combine(id)
    	hasher.combine(pregnancies)
    	hasher.combine(glucose)
    	hasher.combine(bloodPressure)
    	hasher.combine(skinThickness)
    	hasher.combine(insulin)
    	hasher.combine(bmi)
    	hasher.combine(diabetesPedigreeFunction)
    	hasher.combine(age)
    	hasher.combine(outcome)
	  }
	}
	

