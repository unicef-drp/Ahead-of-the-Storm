#!/usr/bin/env python3
"""
Main Impact Analysis Pipeline Orchestrator

This script coordinates the complete impact analysis pipeline for various hazard types.
It serves as the central orchestrator that can handle hurricanes, floods, etc.

Current implementation focuses on hurricane impact analysis:
1. Reads hurricane data directly from Snowflake
2. Runs impact analysis on the data

Usage:
    python main_pipeline.py --hazard hurricane --storm JERRY --date "2025-10-10 00:00:00"
    python main_pipeline.py --hazard hurricane --storm JERRY --date "2025-10-10 00:00:00" --countries DOM
    python main_pipeline.py --hazard hurricane --storm JERRY --date "2025-10-10 00:00:00" --skip-analysis

    python components/data/main_pipeline.py --hazard hurricane --storm MELISSA --date "2025-10-22 00:00:00" --countries NIC
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Add the project root to Python path so we can import components
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import centralized configuration
from .config import config

# Import our custom modules
from impact_analysis import (
    load_envelopes_from_snowflake,
    read_bounding_box,
    is_envelope_in_zone,
    create_views_from_envelopes_in_country
)

# Configure logging
def setup_logging(log_level="INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('main_pipeline.log')
        ]
    )
    return logging.getLogger(__name__)

def run_complete_impact_analysis(storm, date, countries, logger):
    """
    Complete impact analysis orchestration
    
    Args:
        storm: Storm name (e.g., 'JERRY')
        date: Forecast date (e.g., '20251010000000')
        countries: List of country codes (default: ['DOM', 'VNM'])
        logger: Logger instance
    
    Returns:
        dict: Summary of analysis results
    """
    logger.info(f"Running impact analysis for {storm} at {date}")
    logger.info(f"Countries: {', '.join(countries)}")
    
    try:
        # Load envelope data directly from Snowflake
        logger.info("Loading envelope data from Snowflake...")
        gdf_envelopes = load_envelopes_from_snowflake(storm, date)
        
        if gdf_envelopes.empty:
            logger.error(f"No envelope data found for {storm} at {date}")
            return {"success": False, "error": "No envelope data found"}
        
        logger.info(f"Loaded {len(gdf_envelopes)} envelope records")
        logger.info("Envelopes already converted to GeoDataFrame")
        
        # Get bounding box
        logger.info("Getting bounding box...")
        bbox = read_bounding_box()
        
        # Check if envelopes intersect with region
        if not is_envelope_in_zone(bbox, gdf_envelopes):
            logger.error("Envelopes do not intersect with region of interest")
            return {"success": False, "error": "No intersection with region"}
        
        logger.info("Envelopes intersect with region")
        
        # Create impact views for each country
        logger.info("Creating impact views...")
        total_views = 0
        for country in countries:
            create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes)
            total_views += 4  # schools, health centers, tiles, tracks
        
        logger.info("Impact analysis completed successfully")
        return {
            "success": True,
            "envelopes_processed": len(gdf_envelopes),
            "countries_processed": len(countries),
            "total_views_created": total_views
        }
        
    except Exception as e:
        logger.error(f"Error during impact analysis: {str(e)}")
        return {"success": False, "error": str(e)}

class ImpactPipelineStats:
    """Track pipeline execution statistics"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.analysis_success = False
        self.countries_processed = 0
        self.views_created = 0
        self.errors = []
    
    def log_summary(self, logger):
        """Log pipeline execution summary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else 0
        
        logger.info("=" * 70)
        logger.info("IMPACT ANALYSIS PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Execution time: {duration:.2f} seconds")
        logger.info(f"Impact analysis: {'SUCCESS' if self.analysis_success else 'FAILED'}")
        logger.info(f"Countries processed: {self.countries_processed}")
        logger.info(f"Views created: {self.views_created}")
        
        if self.errors:
            logger.error("Errors encountered:")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        logger.info("=" * 70)

def run_hurricane_pipeline(storm, forecast_time, countries=None, skip_analysis=False, log_level="INFO"):
    """
    Run the complete hurricane impact analysis pipeline
    
    Args:
        storm: Storm name (e.g., 'JERRY')
        forecast_time: Forecast time (e.g., '2025-10-10 00:00:00')
        countries: List of country codes (default: ['DOM', 'VNM'])
        skip_analysis: Skip analysis step (for testing)
        log_level: Logging level
    
    Returns:
        ImpactPipelineStats: Pipeline execution statistics
    """
    logger = setup_logging(log_level)
    stats = ImpactPipelineStats()
    stats.start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info("HURRICANE IMPACT ANALYSIS PIPELINE")
    logger.info("=" * 70)
    logger.info(f"Storm: {storm}")
    logger.info(f"Forecast Time: {forecast_time}")
    logger.info(f"Countries: {countries if countries else ['DOM', 'VNM']}")
    logger.info(f"Skip Analysis: {skip_analysis}")
    logger.info("=" * 70)
    
    try:
        # Step 1: Impact Analysis (reads directly from Snowflake)
        if not skip_analysis:
            logger.info("STEP 1: Impact Analysis")
            logger.info("-" * 50)
            
            # Convert forecast time to the format expected by impact analysis
            if isinstance(forecast_time, str) and len(forecast_time) == 19:  # "2025-10-10 00:00:00"
                # Convert to YYYYMMDDHHMMSS format
                dt = datetime.strptime(forecast_time, "%Y-%m-%d %H:%M:%S")
                analysis_date = dt.strftime("%Y%m%d%H%M%S")
            else:
                analysis_date = forecast_time
            
            # Run complete impact analysis orchestration
            analysis_result = run_complete_impact_analysis(storm, analysis_date, countries, logger)
            
            if analysis_result["success"]:
                stats.analysis_success = True
                stats.countries_processed = analysis_result["countries_processed"]
                stats.views_created = analysis_result["total_views_created"]
                logger.info(f"Impact analysis completed successfully")
                logger.info(f"   Envelopes processed: {analysis_result['envelopes_processed']}")
                logger.info(f"   Countries processed: {stats.countries_processed}")
                logger.info(f"   Views created: {stats.views_created}")
            else:
                stats.analysis_success = False
                stats.errors.append(f"Analysis failed: {analysis_result['error']}")
                logger.error(f"Impact analysis failed: {analysis_result['error']}")
        else:
            logger.info("STEP 1: Impact Analysis SKIPPED")
            logger.info("-" * 50)
            stats.analysis_success = True  # Mark as success since we skipped it
            logger.info("Impact analysis step skipped")
    
        # Pipeline completion
        stats.end_time = datetime.now()
        
        if stats.analysis_success:
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Pipeline completed with errors")
        
        stats.log_summary(logger)
        return stats
        
    except Exception as e:
        stats.end_time = datetime.now()
        stats.errors.append(f"Pipeline execution error: {str(e)}")
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        stats.log_summary(logger)
        return stats



# python components/data/main_pipeline.py --hazard hurricane --storm FENGSHEN --date "2025-10-20 00:00:00" --countries VNM


def main():
    parser = argparse.ArgumentParser(description="Run complete impact analysis pipeline")
    
    # Required arguments
    # parser.add_argument("--hazard", required=True, choices=["hurricane"], 
    #                    help="Hazard type (currently supports: hurricane)")
    # parser.add_argument("--storm", required=True, help="Storm name (e.g., JERRY)")
    # parser.add_argument("--date", required=True, help="Forecast time (e.g., '2025-10-10 00:00:00')")
    
    parser.add_argument("--hazard", default="hurricane", 
                       help="Hazard type (currently supports: hurricane)")
    parser.add_argument("--storm", default="MELISSA", help="Storm name (e.g., JERRY)")
    parser.add_argument("--date", default="20251022120000", help="Forecast time (e.g., '2025-10-10 00:00:00')")
    
    # Optional arguments
    parser.add_argument("--countries", nargs="+", default=["NIC"],
                        help="Country codes (e.g., DOM)")
    parser.add_argument("--skip-analysis", action="store_true", 
                       help="Skip analysis step (for testing)")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Run pipeline based on hazard type
    if args.hazard == "hurricane":
        stats = run_hurricane_pipeline(
            storm=args.storm,
            forecast_time=args.date,
            countries=args.countries,
            skip_analysis=args.skip_analysis,
            log_level=args.log_level
        )
    else:
        print(f"Error: Hazard type '{args.hazard}' not yet implemented")
        sys.exit(1)
    
    # Exit with appropriate code
    if stats.analysis_success:
        print("\nPipeline completed successfully!")
        sys.exit(0)
    else:
        print("\nPipeline completed with errors!")
        sys.exit(1)

if __name__ == "__main__":
    main()
