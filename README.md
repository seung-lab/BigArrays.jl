# BigArray.jl
storing and accessing large julia array using different backends.

this package design was inspired by [GPUArray](https://github.com/JuliaGPU/GPUArrays.jl)

## Installation
    Pkg.clone(https://github.com/seung-lab/BigArray.jl.git)
    
## usage
    using H5sBigArray
   
## supported backends
- [x] hdf5 files. 

- [x] seunglab aligned 2D image hdf5 files.

- [ ] [google subvolume](https://developers.google.com/brainmaps/v1beta2/rest/v1beta2/volumes/subvolume)

- [ ] [JPL BOSS](https://github.com/jhuapl-boss)

- [ ] [KLB](http://www.nature.com/nprot/journal/v10/n11/abs/nprot.2015.111.html), [the repo](https://bitbucket.org/fernandoamat/keller-lab-block-filetype)
