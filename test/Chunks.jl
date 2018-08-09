using BigArrays
#using BigArrays.H5sBigArrays
using BigArrays.Chunks
using Test

@testset "test chunks" begin 
    chk = Chunk(rand(Float32, 200,200,20,3), [101,201, 31,1], [4,4,40])
    H5sDir = "/tmp/test.h5sbigarray.aff"                                       
    ba = H5sBigArray(H5sDir; dataType = Float32, blockSize=(256,256,32,3), 
                     chunkSize=(8,8,2,3), globalOffset = (0,0,0,0))                                          
    blendchunk(ba, chk)
    rm(H5sDir, recursive=true)
end # testset
