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
    voxelOffset ::NTuple{3,Int}
end

function InfoScale(d::Dict{Symbol, Any})
    chunkSizes = map(x->tuple(Int.(x)...), d[:chunk_sizes])
    encoding = ENCODING_MAP[ d[:encoding] ]
    key = d[:key] |> Symbol 
    resolution = tuple(Float64.(d[:resolution])...)
    size = tuple(Int.(d[:size])...)
    voxelOffset = tuple(Int.(d[:voxel_offset])...)
    InfoScale(key, chunkSizes, encoding, resolution, size, voxelOffset)
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

############ get the properties ##########
@inline function get_data_type(self::Info)
    self.dataType
end 

@inline function get_chunk_size(self::Info, mip::Integer=0)
    self.scales.chunkSizes[mip+1]
end 

@inline function get_num_channels(self::Info)
    self.numChannels 
end 

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
        voxelOffset = (voxelOffset..., 0)
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
