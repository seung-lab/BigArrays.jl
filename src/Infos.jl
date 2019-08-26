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
    volumeSize  ::NTuple{3,Int}
    voxelOffset ::CartesianIndex{3}
end

"""
    InfoScale(; key::Symbol=:4_4_40, 
                    chunkSizes::Vector{NTuple{3,Int}}=[(64,64,64)],
                    encoding::DataType=GzipCoding,
                    resolution::NTuple{3,Float64}=map(x->Float64(parse(x)), 
                                                      split(string(key), "_")),
                    volumeSize::NTuple{3,Int}=(1024,1024,1024),
                    voxelOffset::CartesianIndex{3}=CartesianIndex{3}(0,0,0))
 
"""
function InfoScale(; key::Symbol=Symbol("4_4_40"), 
                    chunkSizes::Vector{NTuple{3,Int}}=[(64,64,64)],
                    encoding::DataType=GzipCoding,
                    resolution::NTuple{3,Float64}=Tuple(map(x->Float64(Meta.parse(x)), 
                                                            split(string(key), "_"))),
                    volumeSize::NTuple{3,Int}=(1024,1024,1024),
                    voxelOffset::CartesianIndex{3}=CartesianIndex{3}(0,0,0))
    InfoScale(key, chunkSizes, encoding, resolution, volumeSize, voxelOffset)
end 

function InfoScale(d::Dict{Symbol, Any})
    chunkSizes = map(x->tuple(Int.(x)...), d[:chunk_sizes])
    encoding = ENCODING_MAP[ d[:encoding] ]
    key = d[:key] |> Symbol 
    resolution = tuple(Float64.(d[:resolution])...)
    volumeSize = tuple(Int.(d[:size])...)
    voxelOffset = CartesianIndex{3}(Int.(d[:voxel_offset])...)
    InfoScale(key, chunkSizes, encoding, resolution, volumeSize, voxelOffset)
end

function Base.show(io::IO, self::InfoScale)
    show(io, get_key(self))
#    println("chunk size:    ", get_chunk_size(self))
#    println("encoding:      ", get_encoding(self))
#    println("resolution:    ", get_resolution(self))
#    println("volume size:   ", get_volume_size(self))
#    println("voxel offset:  ", get_voxel_offset(self))
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
    d[:size] = [get_volume_size(self)...]
    d[:voxel_offset] = [Tuple(get_voxel_offset(self))...]
    return d
end

@inline function Base.string(self::InfoScale)
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

@inline function set_chunk_size!(self::InfoScale, chunkSize::NTuple{3,Int})
    @assert length(self.chunkSizes) == 1
    self.chunkSizes[1]=chunkSize
end

@inline function get_offset(self::InfoScale)
    self.voxelOffset 
end 
@inline function set_offset(self::InfoScale, offset::CartesianIndex{N}) where N 
    self.voxelOffset = offset 
end 

@inline function get_encoding(self::InfoScale) self.encoding end
@inline function set_encoding!(self::InfoScale, encoding::DataType) 
    self.encoding = encoding 
end

"""
    set_encoding!(self::InfoScale, encoding::Symbol)

the encoding map is: 
$(ENCODING_MAP)
"""
@inline function set_encoding!(self::InfoScale, encoding::String)
    self.encoding = ENCODING_MAP[encoding]
end 

@inline function get_resolution(self::InfoScale) self.resolution end 
@inline function set_resolution!(self::InfoScale, resolution::NTuple{3,T}) where T
    self.resolution = map(Float64, resolution)
end 

@inline function get_volume_size(self::InfoScale) self.volumeSize end 
@inline function set_volume_size!(self::InfoScale, volumeSize::NTuple{3,Int}) 
    self.volumeSize=volumeSize 
end 

@inline function get_voxel_offset(self::InfoScale) self.voxelOffset end
@inline function set_voxel_offset!(self::InfoScale, voxelOffset::CartesianIndex{3}) 
    self.voxelOffset = voxelOffset 
end 

function get_properties(self::InfoScale)
    chunkSize = get_chunk_size(self)
    encoding = get_encoding(self)
    resolution = get_resolution(self)
    voxelOffset = get_voxel_offset(self)
    volumeSize = get_volume_size(self)
    return chunkSize, encoding, resolution, voxelOffset, volumeSize 
end

"""
    generate_next_mip(self::InfoScale)

only downsample the images in XY plane by 2 times. 
meaning increase the resolution by 2x in X and Y axis 
the chunk size will remain the same 
the encoding will also be the same 
"""
function generate_next_mip(self::InfoScale)
    resolution = (self.resolution[1]*2.0, self.resolution[2]*2.0, self.resolution[3])
    key = Symbol("$(round(Int,resolution[1]))_$(round(Int,resolution[2]))_$(round(Int,resolution[3]))")
    chunkSize = get_chunk_size(self)
    encoding = get_encoding(self)
    volumeSize = map(div, get_volume_size(self), (2,2,1))
    voxelOffset = CartesianIndex(map(div, Tuple(get_voxel_offset(self)), (2,2,1)))

    return InfoScale(key, [chunkSize], encoding, resolution, volumeSize, voxelOffset)
end 

end # end of InfoScales module 


using .InfoScales 

mutable struct Info{T} 
    mesh        ::String 
    numChannels ::Int 
    scales      ::Vector{InfoScale}
    skeletons   ::String 
    layerType   ::Symbol
    function Info(dataType::DataType, mesh::String, 
                    numChannels::Int, scales::Vector{InfoScale},
                    skeletons::String, layerType::Symbol)
        new{dataType}(mesh, numChannels, scales, skeletons, layerType)
    end 
end


"""
    Info(; 
        dataType::DataType=UInt8,
        mesh::String="",
        numChannels::Int=1,
        scales::Vector{InfoScale}=[InfoScale()],
        skeletons::String="",
        layerType::Symbol=:image)

layerType: the layer type defined in neuroglancer precomputed format: {image, segmentation}
"""
function Info(; 
                dataType::DataType=UInt8,
                mesh::String="",
                numChannels::Int=1,
                scales::Vector{InfoScale}=[InfoScale()],
                skeletons::String="",
                layerType::Symbol=:image,
                volumeSize::NTuple{3,Int}=(1024,1024,1024),
                chunkSize::NTuple{3,Int}=(64,64,64),
                voxelOffset::CartesianIndex{3}=CartesianIndex{3}((0,0,0)),
                numMip::Int=1)
    infoScale = scales[1]
    InfoScales.set_volume_size!(infoScale, volumeSize) 
    InfoScales.set_chunk_size!(infoScale, chunkSize)
    InfoScales.set_voxel_offset!(infoScale, voxelOffset)

    for mip in 2:numMip
        infoScale = InfoScales.generate_next_mip(infoScale)
        push!(scales, infoScale)
    end 

    Info(dataType, mesh, numChannels, scales, skeletons, layerType)
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

function Base.show(self::Info{T}) where T
    println("\ndata type:   ", T)
    println("mesh:          ", get_mesh(self))
    println("num of channel:", get_num_channels(self))
    println("scales:        ", get_scales(self))
    println("skeletons:     ", get_skeletons(self))
    println("layer type:    ", get_layer_type(self))
end 

"""
    Base.Dict(self::Info)

the transformation follows JSON format 
"""
function Base.Dict(self::Info{T}) where T
    d = Dict{Symbol, Any}()
    for (k,v) in DATATYPE_MAP 
        if v == T
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
@inline function Base.string(self::Info)
    d = Dict(self)
    JSON.json(d)
end 

############ get the properties ##########
@inline function get_mesh(self::Info) self.mesh end 
@inline function set_mesh!(self::Info, mesh::String) self.mesh=mesh end 

@inline function get_key(self::Info, mip::Integer=1)
    InfoScales.get_key( get_scales(self)[mip] )
end 

@inline function get_chunk_size(self::Info, mip::Integer=1) 
    InfoScales.get_chunk_size( self.scales[mip] ) 
end
@inline function set_chunk_size!(self::Info, chunkSize::NTuple{3,Int}, mip::Integer)
    InfoScales.set_chunk_size!( self.scales[mip], chunkSize) 
end 
@inline function set_chunk_size!(self::Info, chunkSize::NTuple{3,Int})
    for infoScale in self.scales 
        InfoScales.set_chunk_size!( infoScale )
    end
end 

@inline function get_offset(self::Info, mip::Integer=1) 
    InfoScales.get_offset( self.scales[mip] ) 
end
@inline function set_offset!(self::Info{T}, offset::CartesianIndex{N}, mip::Integer) where {T,N}
    InfoScales.set_offset!( self.scales[mip], offset) 
end

@inline function get_volume_size(self::Info, mip::Integer=1)
    InfoScales.get_volume_size(self.scales[mip])
end 
@inline function set_volume_size!(self::Info, volumeSize::NTuple{3,Int}, mip::Integer=1)
    InfoScales.set_volume_size!(self.scales[mip], volumeSize)
end 

@inline function get_encoding(self::Info, mip::Integer=1)
    InfoScales.get_encoding( get_scales(self)[mip] )
end 
@inline function set_encoding!(self::Info, encoding::Union{String, DataType})
    for scale in get_scales(self)
        InfoScales.set_encoding!(scale, encoding)
    end 
    nothing 
end

@inline function get_num_channels(self::Info) self.numChannels end 
@inline function set_num_channels!(self::Info, numChannels::Int) self.numChannels=numChannels end 

@inline function get_data_type(info::Info{T}) where T 
    T 
end

@inline function Base.size(self::Info)
    get_volume_size(self, 0)
end

function Base.ndims(info::Info) 
    numChannels = get_num_channels(info)
    if numChannels == 1
        return 3
    else
        return 4
    end
end

@inline function get_scales(self::Info) self.scales end 
@inline function set_scales!(self::Info, scales::Vector{InfoScale}) self.scales=scales end 

@inline function get_skeletons(self::Info) self.skeletons end 
@inline function set_skeletons!(self::Info, skeletons::String) self.skeletons=skeletons end 

@inline function get_layer_type(self::Info) self.layerType end 
@inline function set_layer_type(self::Info, layerType::Symbol) self.layerType=layerType end 

"""
    get_properties_in_mip_level(self::Info, mip::Integer=1)
"""
function get_properties_in_mip_level(self::Info, mip::Integer=1)
    numChannels = get_num_channels(self)
    infoScale = self.scales[mip]
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
            return get_properties_in_mip_level(self, i) 
        end 
    end
    @error "did not find any corresponding mip level with key: " key
end


## TODO: some functions to create info file

end # end of module 
