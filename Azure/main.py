from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI()
#   http://127.0.0.1:8000/docs

class Clase(BaseModel):
    title: str
    author : str
    pag: int
    editorial: Optional[str]

@app.get("/")
def hello():
  return {"Hello world!"}
 
@app.get("/api")
def hello1():
  return {"Hello api!"}

@app.get("/api2/{name}")
def hello2(name):
  return {"Hello " + name }

@app.get("/api3/{id}")
def hello3(id: int):
  return {"id = " : id }

@app.post("/insert")
def insertar(api: Clase):
  return {"message": f" add {api.title} insertado"}