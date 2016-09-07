module H5sBigArrays
using ..BigArrays
using HDF5
using JSON

# import BigArrays.
# include("../types.jl")
# include("../index.jl")
const H5DatasetName = "main"

export H5sBigArray, boundingbox

"""
definition of h5s big array
"""
type H5sBigArray <: AbstractBigArray
  dir             ::AbstractString
  blockSize       ::NTuple{3, Int}
  chunkSize       ::NTuple{3, Int}
  compression     ::Symbol          # deflate || blosc
end

"""
default constructor
"""
function H5sBigArray()
  H5sBigArray(string(tempname(), ".h5sbigarray"))
end

"""
construct from a register file, which defines file architecture
"""
function H5sBigArray(dir::AbstractString; blockSize=(4096, 4096, 512), chunkSize=(128,128,16), compression=:deflate)
  configFile = joinpath(dir, "config.json")
  if isfile(configFile)
    # string format of config
    strreg = readall(configFile)
    config = JSON.parse(strreg, dicttype=Dict{Symbol, Any})
    ba = H5sBigArray(dir, config[:blockSize], config[:chunkSize])
  else
    if !isdir(dir)
      mkdir(dir)
    end
    ba = H5sBigArray(dir, blockSize, chunkSize, compression)
    updateconfigfile(ba)
  end
  ba
end


"""
transform bigarray to string
"""
function ba2dict(ba::H5sBigArray)
  d = Dict{Symbol, Any}()
  d[:fname] = ba.dir
  d[:blockSize] = ba.blockSize
  d[:chunkSize] = ba.chunkSize
  d[:compression] = ba.compression
end

function ba2str(ba::H5sBigArray)
  d = ba2dict(ba)
  JSON.json(d)
end

"""
update the config.json file
"""
function updateconfigfile(ba::H5sBigArray)
  configFile = joinpath(dirname(ba.dir), "config.json")
  if !isdir(ba.dir)
    mkdir(ba.dir)
  end
  str = ba2str(ba)

  # write to text file
  f = open(configFile, "w")
  write(f, str)
  close(f)
end

"""
element type of big array
"""
function Base.eltype(A::H5sBigArray)
  files = readdir(A.dir)
  for file in files
    fname = joinpath(A.dir, file)
    if ishdf5(fname)
      f = h5open(fname)
      ret = eltype(f[H5DatasetName])
      close(f)
      return ret
    end
  end
end

"""
number of dimension
"""
function Base.ndims(A::H5sBigArray)
  for file in readdir(A.dir)
    if ishdf5(file)
      f = h5open(file)
      ret = ndims(f[H5DatasetName])
      close(f)
      return ret
    end
  end
end

"""
bounding box of the whole volume
"""
function boundingbox(A::H5sBigArray)
  x1 = Inf;   x2 = -Inf;
  y1 = Inf;   y2 = -Inf;
  z1 = Inf;   z2 = -Inf;
  for file in readdir(A.dir)
    if ishdf5(file)
      f = h5open(file)
      origin = f["origin"]
      sz = ndims(f[H5DatasetName])

      x1 = min(x1, origin[1])
      y1 = min(y1, origin[2])
      z1 = min(z1, origin[3])
      x2 = max(x2, origin[1]+sz[1]-1)
      y2 = max(y2, origin[2]+sz[2]-1)
      z2 = max(z2, origin[3]+sz[3]-1)
    end
  end
  (Int64(x1):Int64(x2), Int64(y1):Int64(y2), Int64(z1):Int64(z2))
end

bbox(A::H5sBigArray) = boundingbox(A::H5sBigArray)

"""
compute size from bounding box
"""
function Base.size(A::H5sBigArray)
  bb = BoundingBox(A)
  size(bb)
end

function Base.size(A::H5sBigArray, i::Int)
  size(A)[i]
end

function Base.show(A::H5sBigArray)
  println("element type: $(eltype(A))")
  println("size: $(size(A))")
  println("bounding box: $(bbox(A))")
  println("the data is in disk, not shown here.")
end

"""
extract chunk from a bigarray
only works for 3D now.
"""
function Base.getindex(A::H5sBigArray, idxes::Union{UnitRange, Int, Colon}...)
  # only support 3D image now, could support arbitrary dimensions in the future
  @assert length(idxes) == 3 || length(idxes)==4
  # allocate memory
  sx = length(idxes[1])
  sy = length(idxes[2])
  sz = length(idxes[3])
  if length(idxes) == 4
    # only support
    sc = 3
  end
  # create buffer
  if length(idxes)==3
    buf = zeros(eltype(A), (sx,sy,sz))
  else
    buf = zeros(eltype(A), (sx,sy,sz,sc))
  end
  for giz in GlobalIndex(idxes[3], A.blockSize[3])
    for giy in GlobalIndex(idxes[2], A.blockSize[2])
      for gix in GlobalIndex(idxes[1], A.blockSize[1])
        # get block id
        bidx, bidy, bidz = blockid((gix,giy,giz), A.blockSize)
        # get hdf5 file name
        fname = joinpath(A.dir, "chunk_$(bidx)_$(bidy)_$(bidz).h5")
        # if have data fill with data,
        # if not, no need to change, keep as zero
        if isfile(fname) && ishdf5(fname)
          # compute index in hdf5
          blkix, blkiy, blkiz = globalIndexes2blockIndexes((gix,giy,giz), A.blockSize)
          # compute index in buffer
          bufix, bufiy, bufiz = globalIndexes2bufferIndexes((gix,giy,giz), idxes)
          # assign data value, preserve existing value
          info("read ($(gix), $giy, $giz) from ($(blkix), $(blkiy), $(blkiz)) of $(fname) to buffer ($bufix, $bufiy, $bufiz)")
          if length(idxes)==3
            buf[bufix, bufiy, bufiz] = h5read(fname, H5DatasetName, (blkix,blkiy,blkiz))
          else
            f = h5open(fname)
            buf[bufix, bufiy, bufiz, :] = f[H5DatasetName][blkix, blkiy, blkiz, :]
            close(f)
          end
        end
      end
    end
  end
  buf
end


"""
put small array to big array
"""
function Base.setindex!(A::H5sBigArray, buf::Array, idxes::Union{UnitRange, Int, Colon}...)
  # only support 3D now
  @assert ndims(buf)==3 || ndims(buf)==4 || size(buf,4)==3
  @assert length(idxes)==3 || length(idxes)==4
  @assert length(idxes[1]) == size(buf, 1)
  @assert length(idxes[2]) == size(buf, 2)
  @assert length(idxes[3]) == size(buf, 3)

  for giz in GlobalIndex(idxes[3], A.blockSize[3])
    for giy in GlobalIndex(idxes[2], A.blockSize[2])
      for gix in GlobalIndex(idxes[1], A.blockSize[1])
        # get block id
        bidx, bidy, bidz = blockid((gix,giy,giz), A.blockSize)
        # get hdf5 file name
        fname = joinpath(A.dir, "chunk_$(bidx)_$(bidy)_$(bidz).h5")
        # compute index in hdf5
        blkix, blkiy, blkiz = globalIndexes2blockIndexes((gix,giy,giz), A.blockSize)
        # compute index in buffer
        bufix, bufiy, bufiz = globalIndexes2bufferIndexes((gix,giy,giz), idxes)
        # put buffer subarray to hdf5, reserve existing values
        while true
          try
              if isfile(fname) && ishdf5(fname)
                  f = h5open(fname, "r+")
                  dataSet = f[H5DatasetName]
                  @assert eltype(f[H5DatasetName])==eltype(buf)
              else
                  f = h5open(fname, "w")
                  # assign values
                  if A.compression == :deflate
                    if ndims(buf)==3
                      dataSet = d_create(f, H5DatasetName, datatype(eltype(buf)),
                            dataspace(A.blockSize[1], A.blockSize[2], A.blockSize[3]),
                            "chunk", (A.chunkSize[1],A.chunkSize[2],A.chunkSize[3]),
                            "shuffle", (), "deflate", 3)
                    else
                      dataSet = d_create(f, H5DatasetName, datatype(eltype(buf)),
                            dataspace(A.blockSize[1], A.blockSize[2], A.blockSize[3], 3),
                            "chunk", (A.chunkSize[1],A.chunkSize[2],A.chunkSize[3], 3),
                            "shuffle", (), "deflate", 3)
                    end
                  elseif A.compression == :blosc
                    if ndims(buf)==3
                      dataSet = d_create(f, H5DatasetName, datatype(eltype(buf)),
                            dataspace(A.blockSize[1], A.blockSize[2], A.blockSize[3]),
                            "chunk", (A.chunkSize[1],A.chunkSize[2],A.chunkSize[3]),
                            "blosc", 3)
                    else
                      dataSet = d_create(f, H5DatasetName, datatype(eltype(buf)),
                            dataspace(A.blockSize[1], A.blockSize[2], A.blockSize[3], 3),
                            "chunk", (A.chunkSize[1],A.chunkSize[2],A.chunkSize[3], 3),
                            "blosc", 3)
                    end
                  end
              end


              # @show idx
              info("save ($gix, $giy, $giz) from buffer ($bufix, $bufiy, $bufiz) to ($blkix, $blkiy, $blkiz) of $(fname)")
              # @show bufix, bufiy, bufiz
              # @show blkix, blkiy, blkiz
              if ndims(buf)==3
                dataSet[blkix, blkiy, blkiz] = buf[bufix, bufiy, bufiz]
              else
                dataSet[blkix, blkiy, blkiz, :] = buf[bufix, bufiy, bufiz, :]
              end
              close(f)
              break
          catch
              warn("open and write $fname failed, will try 5 seconds later...")
              sleep(5)
          end
        end
      end
    end
  end
end
end
