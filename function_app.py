import azure.functions as func 
from services.process_files_to_landing import process_files_to_landing

app = func.FunctionApp() 

app.register_functions(process_files_to_landing) 