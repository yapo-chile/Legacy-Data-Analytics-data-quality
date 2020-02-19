#!/bin/bash
export BASE_NAME=data-pipeline-base
export REPO_BASENAME=$(git remote show origin | grep "Fetch URL" | awk '{split($0, x, "Yapo/"); split(x[2], y, "."); print y[1]"-"}')
export BASE_PIPELINE_NAME="$1"
export BASE_REPOSITORY=git@github.mpi-internal.com:Yapo/data-pipeline-base.git

rm -Rf ${BASE_NAME}

if [ -d "${BASE_PIPELINE_NAME}" ]; then
    ### Take action if $DIR exists ###
    echo "Pipeline already exists"
else
    git clone --depth=1 ${BASE_REPOSITORY}
    rm -Rf ${BASE_NAME}/.git
    mv ${BASE_NAME}/project-name ${BASE_PIPELINE_NAME}
    rm -Rf ${BASE_NAME}
    grep -rl "data-pipeline-base" ${BASE_PIPELINE_NAME}/* | xargs sed -i '' "s/${BASE_NAME}/${BASE_PIPELINE_NAME}/g"
    grep -rl "data-base-pipeline" ${BASE_PIPELINE_NAME}/* | xargs sed -i '' "s/data-base-pipeline/${REPO_BASENAME}${BASE_PIPELINE_NAME}/g"
    sed -i '' "s/#\ Base\ pipeline/#\ $1\ pipeline/g" ${BASE_PIPELINE_NAME}/README.md
fi

