#__precompile__()
module S3Dicts

using JSON
using AWSCore
using AWSSDK.S3
#using Retry
#using Libz 
using Memoize
import HTTP
import BigArrays.BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 

function __init__()
    global const NEUROGLANCER_CONFIG_FILENAME = "info"
    global const METADATA = Dict{String, String}(
            "Content-Type"      => "binary/octet-stream") 
    global const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]


    if haskey(ENV, "AWS_ACCESS_KEY_ID")
        global const AWS_CREDENTIAL = AWSCore.aws_config()
    elseif isfile("/secrets/aws-secret.json")
        d = JSON.parsefile("/secrets/aws-secret.json")
        global const AWS_CREDENTIAL = AWSCore.aws_config(creds=AWSCredentials(d["AWS_ACCESS_KEY_ID"], d["AWS_SECRET_ACCESS_KEY"]))
    else 
        @warn("did not find AWS credential! set it in environment variables.")
    end 
end 

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
    data = S3.get_object(AWS_CREDENTIAL; Bucket=self.bkt, 
                  Key=joinpath(dirname(self.keyPrefix), "info"))
    return String(data)
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
    arguments = Dict(:Bucket   => h.bkt,
                 :Key      => joinpath(h.keyPrefix, key),
                 :Body     => data, 
                 Symbol("Content-Type")  => METADATA["Content-Type"],
                 Symbol("Content-Encoding") => contentEncoding )

    resp = S3.put_object(AWS_CREDENTIAL, arguments) 
end

function Base.getindex(h::S3Dict, key::AbstractString)
    try 
        data = S3.get_object(AWS_CREDENTIAL; Bucket=h.bkt, Key=joinpath(h.keyPrefix, key))
        return data
    catch err
        @show err 
        if isa(err, AWSCore.AWSException) && err.code == "NoSuchKey"
            throw(KeyError("NoSuchKey in AWS S3: $key"))
        elseif isa(err, HTTP.ClosedError)
            display(err.e)
            rethrow()
        else
            rethrow()
        end 
    end 
end

function Base.delete!( h::S3Dict, key::AbstractString)
    S3.delete_object(AWS_CREDENTIAL; Bucket=h.bkt, Key=joinpath(h.keyPrefix, key))
end

function Base.keys( h::S3Dict )
    S3.list_objects_v2(AWS_CREDENTIAL; Bucket=h.bkt, prefix=h.keyPrefix)
end

function Base.values(h::S3Dict)
    error("normally values are too large to get them all to RAM")
end

function Base.haskey(h::S3Dict, key::String)
    resp = S3.list_objects_v2(AWS_CREDENTIAL; Bucket=h.bkt, prefix=joinpath(h.keyPrefix, key))
    return Meta.parse( resp["KeyCount"] ) > 0
end 

end # end of module S3Dicts
