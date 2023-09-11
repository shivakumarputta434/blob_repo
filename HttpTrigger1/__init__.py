import azure.functions as func

def main(myblob: func.InputStream):
    print("*************************************************file has uploaded*****************************************************")