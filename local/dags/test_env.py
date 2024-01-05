#import os
#from dotenv import load_dotenv
#import google.auth
#load_dotenv()
#project = os.getenv("PROJECT_ID")

matrix = [
    {
        'id':1,
        'nombre':'pepe'
    },
    {
        'id':2,
        'nombre':'maria'
    }
]
print(matrix[:,0])

class coche:
    def __init__(self, marca, modelo) -> None:
        self.marca = marca
        self.modelo = modelo
        self.arrancado = False
