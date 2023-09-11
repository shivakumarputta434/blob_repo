import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    data = {"name":"myra", "age":10, "village":"peddampet"}
    return func.HttpResponse(
            json.dumps(data),
            status_code=200
    )