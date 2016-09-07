using BigArrays
using BigArrays.H5sBigArrays

chk = Chunk(rand(Float32, 200,200,20,3), [101,201, 31], [4,4,40])

ba = H5sBigArray("/tmp/testchunk.bigarray")
blendchunk(ba, chk)
