FROM astro-runtime-epo3.8:6.0.3
# FROM quay.io/astronomer/astro-runtime:6.0.3

# RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
#     /bin/bash ~/miniconda.sh -b && \
#     ~/miniconda3/bin/conda create -y --name snowpark_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.8 numpy pandas && \
#     ~/miniconda3/bin/conda init bash && \
#     ~/miniconda3/bin/conda install -yn snowpark_env 'snowflake-snowpark-python[pandas]'

# ENV PATH=~/miniconda3/bin:$PATH