FROM julia:1.0
LABEL maintainer="Jingpeng Wu <jingpeng.wu@gmail.com>"

RUN apt update
RUN apt install -y -qq build-essential unzip


#RUN julia -e 'Pkg.init()'
#RUN julia -e 'Pkg.add("ImageMagick")'
#RUN julia -e 'Pkg.clone("https://github.com/seung-lab/EMIRT.jl.git")'
RUN julia -e 'import Pkg; Pkg.clone("https://github.com/seung-lab/BigArrays.jl.git"); Pkg.test("BigArrays")'

