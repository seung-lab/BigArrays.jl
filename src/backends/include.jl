#include("BackendBase.jl"); using .BackendBase
#include("H5sBigArrays.jl"); using .H5sBigArrays;
include("BinDicts.jl"); using .BinDicts;

# we can use goofys to mount s3 and google cloud storage 
# and then many cloud storage could be supported by BinDicts automatically!
include("GSDicts.jl"); using GSDicts;
include("S3Dicts.jl"); using S3Dicts;
