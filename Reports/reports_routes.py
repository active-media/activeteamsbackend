from fastapi import APIRouter, Depends, Body, HTTPException
from fastapi.responses import FileResponse
import tempfile
import os
import subprocess
from Reports.lifeclass_report import build_report, read_rows_from_csv

router = APIRouter(prefix="/reports", tags=["Reports"])


@router.post("/life_class_report/generate")
def generate_life_class_report(
    period: str = Body("This Month"),
    campus: str = Body("All Campuses"),
    format: str = Body("Excel"),
):
    """
    Generate the Life Class attendance report Excel workbook.
    
    This endpoint:
    1. Reads attendance CSV data based on the selected period and campus
    2. Runs the lifeclass_report.py generation logic
    3. Returns the generated Excel file for download
    """
    # Determine CSV file paths based on period and campus
    # For now, we'll use sample/mock data paths
    # In production, these would come from your data storage
    
    men_csv = f"data/men_attendance_{period.replace(' ', '_')}_{campus.replace(' ', '_')}.csv"
    women_csv = f"data/women_attendance_{period.replace(' ', '_')}_{campus.replace(' ', '_')}.csv"
    
    # Create temporary output file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        output_path = tmp.name
    
    try:
        # Read data from CSVs
        men_rows = read_rows_from_csv(men_csv) if os.path.exists(men_csv) else []
        women_rows = read_rows_from_csv(women_csv) if os.path.exists(women_csv) else []
        
        # Build the report using the existing lifeclass_report module
        build_report(men_rows, women_rows, output_path)
        
        # Return the generated file
        return FileResponse(
            path=output_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"LifeClass_Report_{period}_{campus}.xlsx"
        )
    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(output_path):
            os.unlink(output_path)
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(e)}")
    finally:
        # Clean up temp file after response
        if os.path.exists(output_path):
            os.unlink(output_path)