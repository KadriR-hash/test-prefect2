from fastapi import FastAPI
from routers import flow

app = FastAPI()


app.include_router(flow.router)


@app.get("/")
async def root():
    return {"message": "Applications"}
