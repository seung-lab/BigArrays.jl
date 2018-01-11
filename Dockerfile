FROM julia:0.6.2
LABEL maintainer="Jingpeng Wu <jingpeng.wu@gmail.com>"

RUN apt update
RUN apt install -y -qq build-essential unzip hdf5-tools


RUN julia -e 'Pkg.init()'
RUN julia -e 'Pkg.clone("https://github.com/seung-lab/EMIRT.jl.git")'
RUN julia -e 'Pkg.clone("https://github.com/seung-lab/BigArrays.jl.git")'
RUN julia -e 'Pkg.clone("https://github.com/seung-lab/GSDicts.jl.git")'
RUN julia -e 'Pkg.clone("https://github.com/seung-lab/S3Dicts.jl.git")'
RUN julia -e 'Pkg.clone("https://github.com/seung-lab/BOSSArrays.jl.git")'

RUN julia -e 'Pkg.test("BigArrays")'
