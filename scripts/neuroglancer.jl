using BigArrays
using GSDicts
d = GSDict("gs://neuroglancer/golden_v0/image/4_4_40")

ba = BigArray(d)

img = ba[1:64,1:64,193:256]

@show img
