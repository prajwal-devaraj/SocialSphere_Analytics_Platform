.PHONY: help up down generate-data pipeline-local dashboard test clean

help:
	@echo "SocialSphere Analytics Platform - common commands"
	@echo ""
	@echo "  make up               Start the full Docker Compose stack"
	@echo "  make down             Stop the stack"
	@echo "  make generate-data    Generate synthetic events into ./data/bronze_local"
	@echo "  make pipeline-local   Run bronze->silver->gold Spark jobs against local data"
	@echo "  make dashboard        Run the Streamlit dashboard locally (no Docker)"
	@echo "  make test             Run the pytest suite"
	@echo "  make clean            Remove generated local data"

up:
	docker-compose up -d

down:
	docker-compose down

generate-data:
	cd event_generator && python3 generate_events.py --events 1000000 --users 50000 --output ../data/bronze_local

pipeline-local:
	cd spark_jobs && python3 bronze_to_silver.py --input "../data/bronze_local/*/*/*" --output ../data/silver_local
	cd spark_jobs && python3 silver_to_gold_daily_metrics.py --input ../data/silver_local --output ../data/gold_local/daily_user_metrics
	cd spark_jobs && python3 retention_cohort_job.py --input ../data/silver_local --output ../data/gold_local/retention_cohorts
	cd spark_jobs && python3 funnel_analysis_job.py --input ../data/silver_local --output ../data/gold_local/funnel_metrics
	cd spark_jobs && python3 feature_usage_job.py --input ../data/silver_local --output ../data/gold_local/feature_usage
	cd spark_jobs && python3 revenue_metrics_job.py --input ../data/silver_local --output ../data/gold_local/revenue_metrics
	cd spark_jobs && python3 anomaly_detection_job.py --daily-metrics ../data/gold_local/daily_user_metrics --revenue-metrics ../data/gold_local/revenue_metrics --output ../data/gold_local/anomaly_detection

dashboard:
	cd dashboard && GOLD_DATA_ROOT=../data/gold_local streamlit run app.py

test:
	pytest tests/ -v

clean:
	rm -rf data/bronze_local data/silver_local data/gold_local
