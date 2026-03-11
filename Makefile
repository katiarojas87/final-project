#.DEFAULT_GOAL := default
#################### PACKAGE ACTIONS ###################

reinstall_package:
	@pip uninstall -y final_project_package || :
	@pip install -e .

streamlit:
	-@streamlit run frontend.app.py

run_load_data:
	python -c 'from final_project_package.interface.main_basic import load_data; import pathlib; load_data(str(pathlib.Path.cwd()), 50)'

run_embeddings:
	python -c 'from final_project_package.interface.main_basic import add_embedding; import pathlib; add_embedding(str(pathlib.Path.cwd()), 50)'

run_preprocess:
	python -c 'from final_project_package.interface.main_basic import preprocess; import pathlib; preprocess(str(pathlib.Path.cwd()), 0.3)'

run_train:
	python -c 'from final_project_package.interface.main_basic import train; import pathlib; train(str(pathlib.Path.cwd()))'

run_evaluate:
	python -c 'from final_project_package.interface.main_basic import evaluate; import pathlib; evaluate(str(pathlib.Path.cwd()))'

run_pred:
	python -c 'from final_project_package.interface.main_basic import pred; import pathlib; pred(str(pathlib.Path.cwd()))'

run_add_pred:
	python -c 'from final_project_package.interface.main_basic import add_prediction; import pathlib; add_prediction(str(pathlib.Path.cwd()))'

run_all: run_preprocess run_train run_evaluate run_add_pred

#run_workflow:
#	PREFECT__LOGGING__LEVEL=${PREFECT_LOG_LEVEL} python -m final_project.interface.workflow

#run_api:
#	uvicorn final_project_package.api.fast:app --reload
