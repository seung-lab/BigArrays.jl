FROM julia:1.0
LABEL maintainer="Jingpeng Wu <jingpeng.wu@gmail.com>"

RUN apt update \
    && apt install -y -qq build-essential unzip \
    #RUN julia -e 'import Pkg; Pkg.init(); Pkg.add("ImageMagick")'
    && julia -e 'using Pkg; Pkg.develop(PackageSpec(url="https://github.com/seung-lab/BigArrays.jl.git")); Pkg.test("BigArrays")'

