import Foundation
import SQLite3

/* Code adapted from https://www.raywenderlich.com/6620276-sqlite-with-swift-tutorial-getting-started */

class DB {
  let dbPointer : OpaquePointer?
  static let dbName = "diabeatsApp.db"
  static let dbVersion = 1

  static let diabeatsTableName = "Diabeats"
  static let diabeatsID = 0
  static let diabeatsCols : [String] = ["TableId", "id", "pregnancies", "glucose", "bloodPressure", "skinThickness", "insulin", "bmi", "diabetesPedigreeFunction", "age", "outcome"]
  static let diabeatsNumberCols = 0

  static let diabeatsCreateSchema =
    "create table Diabeats (TableId integer primary key autoincrement" + 
        ", id VARCHAR(50) not null"  +
        ", pregnancies integer not null"  +
        ", glucose integer not null"  +
        ", bloodPressure integer not null"  +
        ", skinThickness integer not null"  +
        ", insulin integer not null"  +
        ", bmi double not null"  +
        ", diabetesPedigreeFunction double not null"  +
        ", age integer not null"  +
        ", outcome VARCHAR(50) not null"  +
	"" + ")"
	
  private init(dbPointer: OpaquePointer?)
  { self.dbPointer = dbPointer }

  func createDatabase() throws
  { do 
    { 
    try createTable(table: DB.diabeatsCreateSchema)
      print("Created database")
    }
    catch { print("Errors: " + errorMessage) }
  }

  static func obtainDatabase(path: String) -> DB?
  {
    var db : DB? = nil
    if FileAccessor.fileExistsAbsolutePath(filename: path)
    { print("Database already exists")
      do
      { try db = DB.open(path: path)
        if db != nil
        { print("Opened database") }
        else
        { print("Failed to open existing database") }
      }
      catch { print("Error opening existing database") 
              return nil 
            }
    }
    else
    { print("New database will be created")
      do
      { try db = DB.open(path: path)
        if db != nil
        { print("Opened new database") 
          try db!.createDatabase() 
        }
        else
        { print("Failed to open new database") }
      }
      catch { print("Error opening new database")  
              return nil }
    }
    return db
  }

  fileprivate var errorMessage: String
  { if let errorPointer = sqlite3_errmsg(dbPointer)
    { let eMessage = String(cString: errorPointer)
      return eMessage
    } 
    else 
    { return "Unknown error from sqlite." }
  }
  
  func prepareStatement(sql: String) throws -> OpaquePointer?   
  { var statement: OpaquePointer?
    guard sqlite3_prepare_v2(dbPointer, sql, -1, &statement, nil) 
        == SQLITE_OK
    else 
    { return nil }
    return statement
  }
  
  static func open(path: String) throws -> DB? 
  { var db: OpaquePointer?
  
    if sqlite3_open(path, &db) == SQLITE_OK 
    { return DB(dbPointer: db) }
    else 
    { defer 
      { if db != nil 
        { sqlite3_close(db) }
      }
  
      if let errorPointer = sqlite3_errmsg(db)
      { let message = String(cString: errorPointer)
        print("Error opening database: " + message)
      } 
      else 
      { print("Unknown error opening database") }
      return nil
    }
  }
  
  func createTable(table: String) throws
  { let createTableStatement = try prepareStatement(sql: table)
    defer 
    { sqlite3_finalize(createTableStatement) }
    
    guard sqlite3_step(createTableStatement) == SQLITE_DONE 
    else
    { print("Error creating table") 
      return
    }
    print("table " + table + " created.")
  }

  func listDiabeats() -> [DiabeatsVO]
  { 
  	let statement = "SELECT * FROM Diabeats "
  	return setDataDiabeats(statement: statement)
  }

  func createDiabeats(diabeatsvo : DiabeatsVO) throws
  { let insertSQL : String = "INSERT INTO Diabeats (id, pregnancies, glucose, bloodPressure, skinThickness, insulin, bmi, diabetesPedigreeFunction, age, outcome) VALUES (" 

     + "'" + diabeatsvo.getId() + "'" + "," 
     + String(diabeatsvo.getPregnancies()) + "," 
     + String(diabeatsvo.getGlucose()) + "," 
     + String(diabeatsvo.getBloodPressure()) + "," 
     + String(diabeatsvo.getSkinThickness()) + "," 
     + String(diabeatsvo.getInsulin()) + "," 
     + String(diabeatsvo.getBmi()) + "," 
     + String(diabeatsvo.getDiabetesPedigreeFunction()) + "," 
     + String(diabeatsvo.getAge()) + "," 
     + "'" + diabeatsvo.getOutcome() + "'"
      + ")"
    let insertStatement = try prepareStatement(sql: insertSQL)
    defer 
    { sqlite3_finalize(insertStatement)
    }
    sqlite3_step(insertStatement)
  }

  func searchByDiabeatsid(val : String) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE id = " + "'" + val + "'" 
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatspregnancies(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE pregnancies = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsglucose(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE glucose = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsbloodPressure(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE bloodPressure = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsskinThickness(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE skinThickness = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsinsulin(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE insulin = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsbmi(val : Double) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE bmi = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsdiabetesPedigreeFunction(val : Double) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE diabetesPedigreeFunction = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsage(val : Int) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE age = " + String( val )
	  	return setDataDiabeats(statement: statement)
	  }
	  
  func searchByDiabeatsoutcome(val : String) -> [DiabeatsVO]
	  { 
	  	let statement : String = "SELECT * FROM Diabeats WHERE outcome = " + "'" + val + "'" 
	  	return setDataDiabeats(statement: statement)
	  }
	  

  func editDiabeats(diabeatsvo : DiabeatsVO)
  { var updateStatement: OpaquePointer?
    let statement : String = "UPDATE Diabeats SET " 
    + " pregnancies = " + String(diabeatsvo.getPregnancies()) 
    + "," 
    + " glucose = " + String(diabeatsvo.getGlucose()) 
    + "," 
    + " bloodPressure = " + String(diabeatsvo.getBloodPressure()) 
    + "," 
    + " skinThickness = " + String(diabeatsvo.getSkinThickness()) 
    + "," 
    + " insulin = " + String(diabeatsvo.getInsulin()) 
    + "," 
    + " bmi = " + String(diabeatsvo.getBmi()) 
    + "," 
    + " diabetesPedigreeFunction = " + String(diabeatsvo.getDiabetesPedigreeFunction()) 
    + "," 
    + " age = " + String(diabeatsvo.getAge()) 
    + "," 
    + " outcome = '"+diabeatsvo.getOutcome() + "'" 
    + " WHERE id = '" + diabeatsvo.getId() + "'" 

    if sqlite3_prepare_v2(dbPointer, statement, -1, &updateStatement, nil) == SQLITE_OK
    { sqlite3_step(updateStatement) }
    sqlite3_finalize(updateStatement)
  }

  func deleteDiabeats(val : String)
  { let deleteStatementString = "DELETE FROM Diabeats WHERE id = '" + val + "'"
    var deleteStatement: OpaquePointer?
    
    if sqlite3_prepare_v2(dbPointer, deleteStatementString, -1, &deleteStatement, nil) == SQLITE_OK
    { sqlite3_step(deleteStatement) }
    sqlite3_finalize(deleteStatement)
  }


  deinit
  { sqlite3_close(self.dbPointer) }

  func setDataDiabeats(statement: String) -> [DiabeatsVO] {
          var res : [DiabeatsVO] = [DiabeatsVO]()
          let queryStatement = try? prepareStatement(sql: statement)
          
          while (sqlite3_step(queryStatement) == SQLITE_ROW)
          { 
            let diabeatsvo = DiabeatsVO()
            
	      guard let queryResultDiabeatsColId = sqlite3_column_text(queryStatement, 1)
			      else { return res }	      
			      let id = String(cString: queryResultDiabeatsColId)
			      diabeatsvo.setId(x: id)
	      let queryResultDiabeatsColPregnancies = sqlite3_column_int(queryStatement, 2)
			      let pregnancies = Int(queryResultDiabeatsColPregnancies)
			      diabeatsvo.setPregnancies(x: pregnancies)
	      let queryResultDiabeatsColGlucose = sqlite3_column_int(queryStatement, 3)
			      let glucose = Int(queryResultDiabeatsColGlucose)
			      diabeatsvo.setGlucose(x: glucose)
	      let queryResultDiabeatsColBloodPressure = sqlite3_column_int(queryStatement, 4)
			      let bloodPressure = Int(queryResultDiabeatsColBloodPressure)
			      diabeatsvo.setBloodPressure(x: bloodPressure)
	      let queryResultDiabeatsColSkinThickness = sqlite3_column_int(queryStatement, 5)
			      let skinThickness = Int(queryResultDiabeatsColSkinThickness)
			      diabeatsvo.setSkinThickness(x: skinThickness)
	      let queryResultDiabeatsColInsulin = sqlite3_column_int(queryStatement, 6)
			      let insulin = Int(queryResultDiabeatsColInsulin)
			      diabeatsvo.setInsulin(x: insulin)
	      let queryResultDiabeatsColBmi = sqlite3_column_double(queryStatement, 7)
			      let bmi = Double(queryResultDiabeatsColBmi)
			      diabeatsvo.setBmi(x: bmi)
	      let queryResultDiabeatsColDiabetesPedigreeFunction = sqlite3_column_double(queryStatement, 8)
			      let diabetesPedigreeFunction = Double(queryResultDiabeatsColDiabetesPedigreeFunction)
			      diabeatsvo.setDiabetesPedigreeFunction(x: diabetesPedigreeFunction)
	      let queryResultDiabeatsColAge = sqlite3_column_int(queryStatement, 9)
			      let age = Int(queryResultDiabeatsColAge)
			      diabeatsvo.setAge(x: age)
	      guard let queryResultDiabeatsColOutcome = sqlite3_column_text(queryStatement, 10)
			      else { return res }	      
			      let outcome = String(cString: queryResultDiabeatsColOutcome)
			      diabeatsvo.setOutcome(x: outcome)
  
            res.append(diabeatsvo)
          }
          sqlite3_finalize(queryStatement)
          return res
      }
      
}

