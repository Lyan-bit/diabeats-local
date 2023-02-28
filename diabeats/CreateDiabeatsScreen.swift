
import SwiftUI

struct CreateDiabeatsScreen: View {
 
  @State var bean : DiabeatsVO = DiabeatsVO()
  @ObservedObject var model : ModelFacade

  var body: some View {
  	NavigationView {
  		ScrollView {
  	VStack(spacing: 20) {

  VStack(spacing: 20) {
		HStack (spacing: 20) {
		 Text("Id:").bold()
		 TextField("Id", text: $bean.id).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		Text("Pregnancies:").bold()
		TextField("Pregnancies", value: $bean.pregnancies, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		Text("Glucose:").bold()
		TextField("Glucose", value: $bean.glucose, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		Text("BloodPressure:").bold()
		TextField("BloodPressure", value: $bean.bloodPressure, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		Text("SkinThickness:").bold()
		TextField("SkinThickness", value: $bean.skinThickness, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		Text("Insulin:").bold()
		TextField("Insulin", value: $bean.insulin, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
	}.frame(width: 200, height: 30).border(Color.gray)

	}
	VStack(spacing: 20) {
		HStack (spacing: 20) {
		 Text("Bmi:").bold()
		 TextField("Bmi", value: $bean.bmi, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
		 }.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		 Text("DiabetesPedigreeFunction:").bold()
		 TextField("DiabetesPedigreeFunction", value: $bean.diabetesPedigreeFunction, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
		 }.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20) {
		 Text("Age:").bold()
		 TextField("Age", value: $bean.age, format: .number).keyboardType(.decimalPad).textFieldStyle(RoundedBorderTextFieldStyle())
		 }.frame(width: 200, height: 30).border(Color.gray)

		HStack (spacing: 20)  {
		  Text("Outcome:").bold()
		  TextField("Outcome", text: $bean.outcome).textFieldStyle(RoundedBorderTextFieldStyle())
		  }.frame(width: 200, height: 30).border(Color.gray)

}

      HStack(spacing: 20) {
        Button(action: { 
        	self.model.createDiabeats(x: bean)
        } ) { Text("Create") }
        Button(action: { self.model.cancelCreateDiabeats() } ) { Text("Cancel") }
      }.buttonStyle(.bordered)
    }.padding(.top)
     }.navigationTitle("Create Diabeats")
    }
  }
}

struct CreateDiabeatsScreen_Previews: PreviewProvider {
    static var previews: some View {
        CreateDiabeatsScreen(model: ModelFacade.getInstance())
    }
}

