
function fileName2origin( fileName::AbstractString )
    fileName = replace(fileName, "-",  ":")
    fileName = replace(fileName, "_:", "_-")
    secs = split(fileName, "_")
    origin = zeros(Int, length(secs)-2)
    for i in 1:length(origin)
        origin[i] = parse( split(secs[i+1],":")[1] )
    end
    return origin
end

fileName = "block_-1023-0_-48127--47104_-65023--64896_1-3.h5"
fileName2origin(fileName)

using HDF5
using Blosc
Blosc.set_num_threads(1)


H5_DATASET_NAME = "img"
for file in readdir("affinitymap/")
    if  contains(file,"chunk_14337") || contains(file, "chunk_12289") || 
        contains(file,"chunk_13313") || contains(file, "chunk_15361") ||
        contains(file,"chunk_10241") || contains(file, "chunk_16385")
        continue
    end
    file = joinpath("affinitymap/", file)
    if ishdf5(file)
#         @show file
        f = h5open(file)
#         @show size(f[H5_DATASET_NAME])
        if size(f[H5_DATASET_NAME])==(2048,2048,256,3)
            @show file
            origin = [fileName2origin(file)..., 1]
            @show origin
            for gridz in 1:2
                for gridy in 1:2
                    for gridx in 1:2
                        newOrigin = origin .+ [gridx-1, gridy-1,gridz-1,0].* [1024,1024,128,0]
                        newFileName = "1024/block_$(newOrigin[1])-$(newOrigin[1]+1023)_$(newOrigin[2])-$(newOrigin[2]+1023)_$(newOrigin[3])-$(newOrigin[3]+127)_1-3.h5"
                        if isfile(newFileName)
                            continue
                        end
                        newAff = f[H5_DATASET_NAME][newOrigin[1]-origin[1]+1 : newOrigin[1]-origin[1]+1024,
                                                    newOrigin[2]-origin[2]+1 : newOrigin[2]-origin[2]+1024,
                                                    newOrigin[3]-origin[3]+1 : newOrigin[3]-origin[3]+128, 1:3]
                        @show size(newAff)
#                         h5write(newFileName, H5_DATASET_NAME, newAff)
                        newf = h5open(newFileName, "w")
                        newf["origin"] = newOrigin
                        dataSet = d_create(newf, H5_DATASET_NAME, datatype(eltype(newAff)),
                                            dataspace(1024,1024,128,3),
                                            "chunk", (256,256,32,3),
                                            "shuffle", (), "deflate", 3)
                        dataSet[:,:,:,:] = newAff
                        close(newf)
                        
                    end
                end
            end
        end
        close(f)
        rm(file)
    end
end


