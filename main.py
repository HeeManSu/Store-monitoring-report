from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse, JSONResponse
import database_models
from database import Session, engine
import uuid
import os

from report_generation import generate_store_report
app = FastAPI(title="Store Monitoring System", version="1.0.0")
database_models.Base.metadata.create_all(bind=engine)

def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def greet():
    return {"message": "Welcome to Store Monitoring System API"}


@app.post("/trigger_report")
def trigger_report():
    try:
        report_id = str(uuid.uuid4())
        generate_store_report.delay(report_id)
        
        return {
            "report_id": report_id,
            "status": "Queued",
            "message": "Report generation started in background"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting report generation: {str(e)}")


@app.get("/get_report/{report_id}")
def get_report(report_id: str):
    db = Session()
    try:
        report_record = db.query(database_models.ReportDownloads).filter(
            database_models.ReportDownloads.report_name == report_id
        ).first()

        if report_record.status in ["Running", "Pending"]:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "Running",
                    "message": "Report is still being generated. Please try again later."
                }
            )

        if report_record.status == "Completed":
            if report_record.report_file_path and os.path.exists(report_record.report_file_path):
                return FileResponse(
                    report_record.report_file_path,
                    media_type="text/csv",
                    filename=f"report_{report_id}.csv"
                )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "Error", "message": str(e)}
        )

    finally:
        db.close()

