.PHONY: fetch, unzip, headers, cluster, dask

unzip:
	mkdir -p data/08 && \
	mkdir -p data/10 && \
	mkdir -p data/12 && \
	mkdir -p data/14 && \
	mkdir -p data/16 && \
	unzip 'data/*08.zip' -d data/12 && \
	unzip 'data/*10.zip' -d data/12 && \
	unzip 'data/*12.zip' -d data/12 && \
	unzip 'data/*14.zip' -d data/14 && \
	unzip 'data/*16.zip' -d data/16

headers:
	curl -o data/cm_header.csv http://classic.fec.gov/finance/disclosure/metadata/cm_header_file.csv
	curl -o data/cn_header.csv http://classic.fec.gov/finance/disclosure/metadata/cn_header_file.csv
	curl -o data/ccl_header.csv http://classic.fec.gov/finance/disclosure/metadata/ccl_header_file.csv
	curl -o data/oth_header.csv http://classic.fec.gov/finance/disclosure/metadata/oth_header_file.csv
	curl -o data/pas2_header.csv http://classic.fec.gov/finance/disclosure/metadata/pas2_header_file.csv
	curl -o data/indiv_header.csv http://classic.fec.gov/finance/disclosure/metadata/indiv_header_file.csv
	curl -o data/oppexp_header.csv http://classic.fec.gov/finance/disclosure/metadata/oppexp_header_file.csv

cluster:
	gcloud container clusters create dask-demo \
		--num-nodes=3 \
		--machine-type=n1-standard-2 \
		--zone=us-central1-b \
		--cluster-version=1.8.4-gke.1

helm:
	kubectl --namespace kube-system create sa tiller && \
	kubectl create clusterrolebinding tiller --clusterrole cluster-admin --serviceaccount=kube-system:tiller && \
	helm init --service-account tiller && \
	helm repo add dask https://dask.github.io/helm-chart && \
	helm repo update

dask:
	helm install -f config.yaml dask/dask

article-1-cluster.html: article-1-cluster.ipynb
	jupyter nbconvert article-1-cluster.ipynb --TagRemovePreprocessor.remove_input_tags='{"remove_input"}' --TagRemovePreprocessor.remove_cell_tags='{"remove_cell"}'
