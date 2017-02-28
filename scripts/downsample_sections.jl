using BigArrays
using BigArrays.H5sBigArrays

ba = H5sBigArray("~/seungmount/research/Jingpeng/14_zfish/affinitymap/");

bb = boundingbox(ba)
@show bb

# get section
sec = ba[14337:84992, 11265:47104, 16385,1]

# downsample
using Images
using FixedPointNumbers
sec = reinterpret(N0f8, sec)
im = Images.restrict(sec, (1,2))
