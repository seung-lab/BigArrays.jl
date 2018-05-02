#__precompile__()
module S3Dicts

using JSON
using AWSCore
using AWSS3
using Retry
#using Libz 
using Memoize
import HTTP
import BigArrays.BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 

const NEUROGLANCER_CONFIG_FILENAME = "info"
try 
    #AWSCore.set_debug_level(2)
    global const AWS_CREDENTIAL = AWSCore.aws_config()
catch err
    if isfile("/secrets/aws-secret.json")
        d = JSON.parsefile("/secrets/aws-secret.json")
        global const AWS_CREDENTIAL = AWSCore.aws_config(creds=AWSCredentials(d["AWS_ACCESS_KEY_ID"], d["AWS_SECRET_ACCESS_KEY"]))
    else 
        warn("did not find AWS credential! set it in environment variables.")
    end 
end 

const METADATA = Dict{String, String}(
            "Content-Type"      => "binary/octet-stream") 
const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]

export S3Dict

struct S3Dict <: AbstractBigArrayBackend
    bkt         ::String
    keyPrefix   ::String
end

"""
    S3Dict( dir::String )
construct S3Dict from a directory path of s3
"""
function S3Dict( path::String )
    path = replace(path, "s3://", "")
    bkt, keyPrefix = split(path, "/", limit = 2)
    keyPrefix = strip(keyPrefix, '/')
    S3Dict(bkt, keyPrefix)
end


function get_info(self::S3Dict)
    s3_get(AWS_CREDENTIAL, self.bkt, joinpath(dirname(self.keyPrefix), "info"); 
           retry=true)
end 

function get_scale_name(self::S3Dict)  basename( self.keyPrefix ) end 

function Base.show( self::S3Dict )  show( joinpath(self.bkt, self.keyPrefix) ) end 

function Base.setindex!(h::S3Dict, v::Array, key::AbstractString)
    #@assert startswith(h.dir, "s3://")
    data = reinterpret(UInt8, v[:])
    local contentEncoding::String 
    if all(data[1:3].== GZIP_MAGIC_NUMBER)
        contentEncoding = "gzip"
    else 
        contentEncoding = ""
    end 
    @repeat 4 try 
        resp = s3_put(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key), 
                      data,
                      METADATA["Content-Type"],
                      contentEncoding)
    catch err 
        println("catch error while saving: $err")
        @show typeof(err)
        @show err 
        @delay_retry if true end
    end
end

function Base.getindex(h::S3Dict, key::AbstractString)
    try 
        data = AWSS3.s3_get(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key); 
                        raw=true, retry =false)
        return data
    catch err
        if isa(err, AWSCore.AWSException) && err.code == "NoSuchKey"
            throw(KeyError("NoSuchKey: $key"))
        elseif isa(err, HTTP.ClosedError)
            display(err.e)
            rethrow()
        else
            rethrow()
        end 
    end 
end

function Base.delete!( h::S3Dict, key::AbstractString)
    s3_delete(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key))
end

function Base.keys( h::S3Dict )
    s3_list_objects(AWS_CREDENTIAL, h.bkt, h.keyPrefix)
end

function Base.values(h::S3Dict)
    error("normally values are too large to get them all to RAM")
end

function Base.haskey(h::S3Dict, key::String)
    s3_exists(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key))
end 

end # end of module S3Dicts
