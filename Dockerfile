FROM julia:1.0
LABEL maintainer="Jingpeng Wu <jingpeng.wu@gmail.com>"

RUN apt update
RUN apt install -y -qq build-essential unzip


#RUN julia -e 'import Pkg; Pkg.init(); Pkg.add("ImageMagick")'
RUN julia -e 'import Pkg; Pkg.clone("https://github.com/seung-lab/BigArrays.jl.git"); Pkg.test("BigArrays")'

