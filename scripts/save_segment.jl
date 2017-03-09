using BigArrays

using GSDicts

d1 = GSDict("gs://neuroglancer/snemi3dtest_v0/segmentation/6_6_30/")

ba1 = BigArray(d1);

data = d1["0-64_192-256_0-64"]

data = reinterpret(eltype(ba1), data)

img = reshape(data, (64,64,64))

@assert img == ba1[1:64,193:256, 1:64]

# test folder
using BigArrays
using GSDicts
d2 = GSDict("gs://neuroglancer/snemi3dtest_v0/segmentation_jwu/6_6_30/")
ba2 = BigArray(d2);
ba2[1:64, 193:256, 1:64] = img

img2 = ba2[1:64, 193:256, 1:64]

@assert img2 == img
