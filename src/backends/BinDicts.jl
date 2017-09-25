module BinDicts
using Libz
using JSON
using BigArrays
import BigArrays.DATATYPE_MAP

export BinDict 

immutable BinDict <: Associative  
    path::String
    configDict::Dict{Symbol, Any}
end 

"""
BinDict should follow the format of neuroglancer precomputed. the dataset structure will be the same with neuroglancer, which means copying dataset directly to the cloud should make it available immediately.
"""
function BinDict(path::AbstractString)
    infoPath = joinpath(path, "../info")
    configDict = JSON.parsefile(infoPath, dicttype=Dict{Symbol, Any})
    configDict[:dataType] =  DATATYPE_MAP[ configDict[:data_type] ]

    # the name of the mip level
    mipName = basename(path)
    for d in configDict[:scales]
        if d[:key] == mipName
            if configDict[:num_channels] == 1
                configDict[:chunkSize] = d[:chunk_sizes][1]
            else 
                configDict[:chunkSize] = [d[:chunk_sizes][1]..., configDict[:num_channels]]
            end
            configDict[:offset] = d[:voxel_offset]
        end 
    end 
    BinDict(path, configDict)
end

function get_path(self::BinDict)
    self.path 
end 

function Base.getindex( self::BinDict, key::AbstractString)
    f = open( joinpath( get_path(self), key ))
    data = read(f)
    close(f)
    Libz.inflate(data)
end 

function Base.setindex!( self::BinDict, value::Array, key::AbstractString )
    data = Libz.deflate(reinterpret(UInt8, value[:]))
    fileName = joinpath( get_path(self), key )
    f = open(fileName, "w")
    write(f, data)
    close(f)
end 

end # module
