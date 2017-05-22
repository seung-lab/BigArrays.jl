module AlignedBigArrays
using ..BigArrays
using HDF5

include("../types.jl")

const WAIVER_ID = 1
const H5_DATASET_NAME = "img"
const H5_DATASET_ELEMENT_TYPE = UInt8
const DATASET_NDIMS = 3
# const GLOBAL_OFFSET = [16384,16384,16384]

export AlignedBigArray, boundingbox

# register item of one section / hdf5 file
typealias Tsecreg Dict{Symbol, Union{AbstractString, Int}}
# the whole register records filename, xstart, ystart, xdim, ydim
typealias Tregister Dict{Int, Tsecreg}

type AlignedBigArray{T, N} <: AbstractBigArray
    register::Tregister
    function (::Type{AlignedBigArray})( register::Tregister )
        new{UInt8, 3}(register)
    end
end

"""
construct from a register file,
which was the final output of registration pipeline in seunglab
"""
function AlignedBigArray(fregister::AbstractString)
    if isdir( fregister )
        fregister = joinpath(fregister, "registry.txt")
    end
    @assert isfile(fregister)
    
    f = open(fregister)
    lines = readlines(f)
    close(f)
    register = Tregister()
    # sizehint!(Tregister, length(lines))
    z = 0
    for i in eachindex(lines)
        z += 1
        # initialize the registration of a section image
        d = Tsecreg()
        line = lines[i]
        if length(split(line)) == 7
            registerFile, tmpZero, xoff, yoff, xdim, ydim, tf = split(line)
        elseif length(split(line))==6
            registerFile, xoff, yoff, xdim, ydim, tf = split(line)
        else
            error("unsupported format of register file: $(line)")
        end
        d[:secIDinWaver] = parse(split(split(registerFile,'_')[1], ',')[2])
        d[:waiverID] = parse(split(split(registerFile,'_')[1], ',')[1])
        d[:registerFile] = joinpath(dirname(fregister), registerFile * ".h5")
        d[:xoff] = parse(xoff)
        d[:yoff] = parse(yoff)
        d[:zoff] = z-1
        d[:xdim] = parse(xdim)
        d[:ydim] = parse(ydim)
        d[:zdim] = 1
        register[z] = d
    end
    AlignedBigArray(register)
end

"""
specialized for UInt8 raw image data type
"""
function Base.eltype{T,N}(A::AlignedBigArray{T,N})
    return T
    # return H5_DATASET_ELEMENT_TYPE
    # for key in keys(A.register)
    #     registerFile = A.register[key][:registerFile]
    #     if isfile(registerFile) && ishdf5(registerFile)
    #         f = h5open(registerFile)
    #         ret = eltype(f[])
    #         close(f)
    #         return ret
    #     end
    # end
end

"""
number of dimension
"""
function Base.ndims(A::AlignedBigArray)
    return DATASET_NDIMS
    # for key in keys(A.register)
    #     registerFile = A.register[key][:registerFile]
    #     if isfile(registerFile) && ishdf5(registerFile)
    #         f = h5open(registerFile)
    #         ret = ndims(f[H5_DATASET_NAME])
    #         close(f)
    #         # don't forget the z dimension!
    #         return ret+1
    #     end
    # end
end

"""
bounding box of the whole volume
"""
function boundingbox(A::AlignedBigArray; zmin=-Inf, zmax =Inf)
    x1 = Inf;   x2 = -Inf;
    y1 = Inf;   y2 = -Inf;
    z1 = Inf;   z2 = -Inf;
    for d in values( A.register )
        if d[:zoff] < zmin-1 || d[:zoff] > zmax-1
            continue
        end
        @show d[:zoff]
        x1 = min(x1, d[:xoff] + 1)
        y1 = min(y1, d[:yoff] + 1)
        z1 = min(z1, d[:zoff] + 1)
        x2 = max(x2, d[:xoff] + d[:xdim])
        y2 = max(y2, d[:xoff] + d[:ydim])
        z2 = max(z2, d[:zoff] + d[:zdim])
    end
    (Int64(x1):Int64(x2), Int64(y1):Int64(y2), Int64(z1):Int64(z2))
end

"""
compute size from bounding box
"""
function Base.size(A::AlignedBigArray)
    bb = boundingbox(A)
    map(length, bb)
end

function Base.size(A::AlignedBigArray, i::Int)
    size(A)[i]
end

function Base.show(A::AlignedBigArray)
    println("type: $(typeof(A))")
    println("bounding box: $(boundingbox(A))")
    println("the data is in disk, can not ")
end

"""
read out part of the image from HDF5 file
"""
function read_subimage!(buf,
                        registerFile::AbstractString,
                        sizeX::Integer,
                        sizeY::Integer,
                        xidxSection::Union{Int, UnitRange},
                        yidxSection::Union{Int, UnitRange},
                        zidxSection::Integer)
    @assert ishdf5(registerFile)

    # the explicit coordinate range
    x1 = max(1, first(xidxSection));   x2 = min(sizeX, last(xidxSection));
    y1 = max(1, first(yidxSection));   y2 = min(sizeY, last(yidxSection));
    if x1>x2 || y1>y2
        warn("no overlaping region in this section: $(registerFile)")
    else
        # index in buffer
        bufx1 = x1 - first(xidxSection) + 1;
        bufx2 = x2 - first(xidxSection) + 1;
        bufy1 = y1 - first(yidxSection) + 1;
        bufy2 = y2 - first(yidxSection) + 1;
        buf[bufx1:bufx2, bufy1:bufy2, zidxSection] = h5read(registerFile, H5_DATASET_NAME, (x1:x2, y1:y2))
    end
end

function Base.getindex(A::AlignedBigArray, r::CartesianRange)
    getindex(A, cartesian_range2unitrange(r)...)
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(A::AlignedBigArray, idxes::Union{UnitRange, Int}...)
    @assert length(idxes) == 3
    # allocate memory
    sx = length(idxes[1])
    sy = length(idxes[2])
    sz = length(idxes[3])
    # create buffer
    buf = zeros(H5_DATASET_ELEMENT_TYPE, (sx,sy,sz))
    @show idxes
    for z in idxes[3]
        if haskey(A.register, z)
            registerFile = A.register[z][:registerFile]
            xidxSection = idxes[1] - A.register[z][:xoff]
            yidxSection = idxes[2] - A.register[z][:yoff]
            zidxSection = z  - first(idxes[3]) + 1
            # println("registerFile: $(basename(registerFile)), xidx: $(xidx), yidx: $(yidx), zidx: $(zidx)")
            read_subimage!( buf,
                            registerFile,
                            A.register[z][:xdim],
                            A.register[z][:ydim],
                            xidxSection, yidxSection, zidxSection)
        else
            warn("section file not exist: $z")
        end
    end
    buf
end

function Base.CartesianRange( ba::AlignedBigArray, z::Int )
    d = ba.register[z]
    start = CartesianIndex( d[:xoff]+1, d[:yoff]+1, z )
    stop  = CartesianIndex( d[:xoff]+d[:xdim], d[:yoff]+d[:ydim], z)
    return CartesianRange( start, stop )
end

end # end of module AlignedBackend
