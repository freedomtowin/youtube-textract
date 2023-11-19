# youtube-textract

### Steps to Run

I am hiding my AWS secrets in a file called local_setup.sh. This will be called by the development Docker environment. This file is not included in the GitHub repository. 

1. Create `local_setup.sh` in the local repo and populate the AWS key information.

```
#!/bin/bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -u awscliv2.zip

sleep 5

if [ -d aws ]; then
	./aws/install
fi

aws configure set aws_access_key_id ""
aws configure set aws_secret_access_key ""
aws configure set region ""
aws configure set output ""

apt install nano

apt install ufw -y
ufw allow 9999
ufw allow 5000

pip install jupyter notebook ipykernel
```

2. run `docker compose -f docker-compose-dev.yml up`, the docker instance will be built and a jupyter notebook will be created on port 8889:9999.
