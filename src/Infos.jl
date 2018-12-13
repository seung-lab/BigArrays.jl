module Infos 
using JSON
export Info 

# map datatype of python to Julia 
const DATATYPE_MAP = Dict{String, DataType}(
    "bool"      => Bool,
    "uint8"     => UInt8, 
    "uint16"    => UInt16, 
    "uint32"    => UInt32, 
    "uint64"    => UInt64, 
    "float32"   => Float32, 
    "float64"   => Float64 
)  


module InfoScales 

using BigArrays.Codings

export InfoScale 

const ENCODING_MAP = Dict{String,Any}(
    # note that the raw encoding in cloud storage will be automatically gzip encoded!
    "raw"       => GzipCoding,
    "jpeg"      => JPEGCoding,
    "blosclz"   => BlosclzCoding,
    "gzip"      => GzipCoding, 
    "zstd"      => ZstdCoding 
)


mutable struct InfoScale  
    key         ::Symbol 
    chunkSizes  ::Vector{NTuple{3,Int}}
    encoding    ::DataType  
    resolution  ::NTuple{3,Float64}
    size        ::NTuple{3,Int}
    voxelOffset ::CartesianIndex{3}
end

"""
    InfoScale(; key::Symbol=:4_4_40, 
                    chunkSizes::Vector{NTuple{3,Int}}=[(64,64,64)],
                    encoding::DataType=GzipCoding,
                    resolution::NTuple{3,Float64}=map(x->Float64(parse(x)), 
                                                      split(string(key), "_")),
                    size::NTuple{3,Int}=(1024,1024,1024),
                    voxelOffset::CartesianIndex{3}=CartesianIndex{3}(0,0,0))
 
"""
function InfoScale(; key::Symbol=:4_4_40, 
                    chunkSizes::Vector{NTuple{3,Int}}=[(64,64,64)],
                    encoding::DataType=GzipCoding,
                    resolution::NTuple{3,Float64}=map(x->Float64(parse(x)), 
                                                      split(string(key), "_")),
                    size::NTuple{3,Int}=(1024,1024,1024),
                    voxelOffset::CartesianIndex{3}=CartesianIndex{3}(0,0,0))
    InfoScale(key, chunkSizes, encoding, resolution, size, voxelOffset)
end 

function InfoScale(d::Dict{Symbol, Any})
    chunkSizes = map(x->tuple(Int.(x)...), d[:chunk_sizes])
    encoding = ENCODING_MAP[ d[:encoding] ]
    key = d[:key] |> Symbol 
    resolution = tuple(Float64.(d[:resolution])...)
    size = tuple(Int.(d[:size])...)
    voxelOffset = CartesianIndex{3}(Int.(d[:voxel_offset])...)
    InfoScale(key, chunkSizes, encoding, resolution, size, voxelOffset)
end

function Base.Dict(self::InfoScale)
    d = Dict{Symbol,Any}()
    d[:key] = get_key(self)
    d[:chunk_sizes] = [[get_chunk_size(self)...]]
    for (k,v) in ENCODING_MAP
        if v == get_encoding(self)
            d[:encoding] = string(k)
        end 
    end 
    d[:size] = [get_size(self)...]
    d[:voxelOffset] = [Tuple(get_voxel_offset(self))...]
    return d
end

function Base.string(self::InfoScale)
    d = Dict(self)
    JSON.json(d)
end

@inline function get_key(self::InfoScale)
    self.key 
end 

@inline function get_chunk_size(self::InfoScale)
    @assert length(self.chunkSizes) == 1
    self.chunkSizes[1] 
end 

@inline function get_encoding(self::InfoScale)
    self.encoding 
end 

@inline function get_resolution(self::InfoScale)
    self.resolution 
end 

@inline function get_size(self::InfoScale)
    self.size 
end 

@inline function get_voxel_offset(self::InfoScale)
    self.voxelOffset 
end 

function get_properties(self::InfoScale)
    chunkSize = get_chunk_size(self)
    encoding = get_encoding(self)
    resolution = get_resolution(self)
    voxelOffset = get_voxel_offset(self)
    size = get_size(self)
    return chunkSize, encoding, resolution, voxelOffset, size 
end 

end # end of InfoScales module 


using .InfoScales 

mutable struct Info 
    dataType    ::DataType 
    mesh        ::String 
    numChannels ::Int 
    scales      ::Vector{InfoScale}
    skeletons   ::String 
    layerType   ::Symbol
end

function Info(d::Dict{Symbol,Any})
    dataType = DATATYPE_MAP[ d[:data_type] ]

    mesh = get(d, :mesh, "")
    numChannels = d[:num_channels]
    scales = map(InfoScale, d[:scales])
    skeletons = get(d, :skeletons, "")
    layerType = Symbol( d[:type] )
    Info(dataType, mesh, numChannels, scales, skeletons, layerType)
end

@inline function Info(str::AbstractString)
    d = JSON.parse(str, dicttype=Dict{Symbol, Any})
    Info(d)
end

@inline function Info(data::Vector{UInt8})
    Info(String(data))
end

"""
    Base.Dict(self::Info)

the transformation follows JSON format 
"""
function Base.Dict(self::Info)
    d = Dict{Symbol, Any}()
    for (k,v) in DATATYPE_MAP 
        if v == get_data_type(self)
            d[:data_type] = string(k)
        end 
    end 
    
    if !isempty(get_mesh(self)) 
        d[:mesh] = get_mesh(self)
    end 

    d[:num_channels] = get_num_channels(self)
    d[:scales] = map(Dict, get_scales(self))
    d[:skeletons] = string(get_skeletons(self))
    d[:type] = string(get_layer_type(self))
    return d
end 

"""
    Base.string(self::Info)

"""
function Base.string(self::Info)
    d = Dict(self)
    JSON.json(d)
end 

############ get the properties ##########
@inline function get_data_type(self::Info) self.dataType end 

@inline function get_mesh(self::Info) self.mesh end 

@inline function get_chunk_size(self::Info, mip::Integer=0) 
    self.scales.chunkSizes[mip+1]
end 

@inline function get_num_channels(self::Info) self.numChannels end 

@inline function get_scales(self::Info) self.scales end 

@inline function get_skeletons(self::Info) self.skeletons end 

@inline function get_layer_type(self::Info) self.layerType end 

"""
    get_properties_in_mip_level(self::Info, mip::Integer=0)
"""
function get_properties_in_mip_level(self::Info, mip::Integer=0)
    numChannels = get_num_channels(self)
    infoScale = self.scales[mip+1]
    chunkSize, encoding, resolution, voxelOffset, volumeSize = 
                                        InfoScales.get_properties(infoScale)
    if numChannels > 1
        chunkSize = (chunkSize..., numChannels)
        volumeSize = (volumeSize..., numChannels)
        voxelOffset = CartesianIndex(Tuple(voxelOffset)..., 0)
    end 
    return chunkSize, encoding, resolution, voxelOffset, volumeSize 
end 

"""
    get_properties_in_mip_level(self::Info, key::Symbol)
"""
function get_properties_in_mip_level(self::Info, key::Symbol)
    for (i, infoScale) in self.scales |> enumerate 
        if key == InfoScales.get_key(infoScale)
            mip = i - 1
            return get_properties_in_mip_level(self, mip) 
        end 
    end
    @error "did not find any corresponding mip level with key: " key
end


## TODO: some functions to create info file

end # end of module 
