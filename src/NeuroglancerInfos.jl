module NeuroglancerInfos
using JSON

export NeuroglancerInfo 

# map datatype of python to Julia 
global const DATATYPE_MAP = Dict{String, DataType}(
    "bool"      => Bool,
    "uint8"     => UInt8, 
    "uint16"    => UInt16, 
    "uint32"    => UInt32, 
    "uint64"    => UInt64, 
    "float32"   => Float32, 
    "float64"   => Float64 
)  

module MipLevelInfos 

struct MipLevelInfo 
    encoding            ::String 
    chunkSizes          ::Vector{NTuple{3,Int}}
    key                 ::String 
    resolution          ::NTuple{3,Int}
    voxelOffset         ::NTuple{3,Int}
    size                ::NTuple{3,Int}
end 

@inline function MipLevelInfo( d::Dict{Symbol, Any} )
    chunkSizes = Vector{NTuple{3,Int}}()
    for sz in d[:chunk_sizes]
        push!(chunkSizes, (sz...))
    end 
    MipLevelInfo(d[:encoding], d[:key], (d[:resolution]...), (d[:size]...), 
                                                    chunkSizes, (d[:voxel_offset]...))
end 

end # module of MipLevelInfos

struct NeuroglancerInfo 
    numChannels         ::Int 
    layerType           ::String 
    dataType            ::DataType 
    scales              ::Vector{MipLevelInfo}
    skeletons           ::String 
    mesh                ::String 
end 

function NeuroglancerInfo( str::String )
    JSON.parse(str, dicttype=Dict{Symbol, Any}) |> Info 
end 
function NeuroglancerInfo( d::Dict )
    mipLevelInfoList = Vector{MipLevelInfo}()
    for mip in d[:scales]
        push!(mipLevelInfoList, MipLevelInfo(mip))
    end
    NeuroglancerInfo( d[:num_channels], d[:type], DATATYPE_MAP[d[:data_type]], mipLevelInfoList, d[:skeletons], d[:mesh])
end 

end # module
