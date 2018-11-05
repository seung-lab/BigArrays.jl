#__precompile__()
module S3Dicts

using JSON
using AWSCore
#using AWSSDK.S3
using AWSS3
#using Retry
import HTTP
import BigArrays.BackendBase: AbstractBigArrayBackend, get_info, get_scale_name 

global const NEUROGLANCER_CONFIG_FILENAME = "info"
global const CONTENT_TYPE = "binary/octet-stream" 
global const GZIP_MAGIC_NUMBER = UInt8[0x1f, 0x8b, 0x08]


if haskey(ENV, "AWS_ACCESS_KEY_ID")
    global AWS_CREDENTIAL = AWSCore.aws_config()
elseif isfile("/secrets/aws-secret.json")
    d = JSON.parsefile("/secrets/aws-secret.json")
    global AWS_CREDENTIAL = AWSCore.aws_config(creds=AWSCredentials(d["AWS_ACCESS_KEY_ID"], d["AWS_SECRET_ACCESS_KEY"]))
else 
    @warn("did not find AWS credential! set it in environment variables.")
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
    path = replace(path, "s3://" => "")
    bkt, keyPrefix = split(path, "/", limit = 2)
    keyPrefix = rstrip(keyPrefix, '/')
    S3Dict(bkt, keyPrefix)
end


function get_info(self::S3Dict)
    #data = S3.get_object(AWS_CREDENTIAL; Bucket=self.bkt, 
    #              Key=joinpath(dirname(self.keyPrefix), "info"))
    data = s3_get(AWS_CREDENTIAL, self.bkt, joinpath(dirname(self.keyPrefix), "info"))
    return String(data)
end 

function get_scale_name(self::S3Dict)  basename( self.keyPrefix ) end 

function Base.show( self::S3Dict )  show( joinpath(self.bkt, self.keyPrefix) ) end 

function Base.setindex!(h::S3Dict, v::Array, key::AbstractString)
    #@assert startswith(h.dir, "s3://")
    data = reinterpret(UInt8, v[:]) |> Array
    local contentEncoding::String 
    if all(data[1:3].== GZIP_MAGIC_NUMBER)
        contentEncoding = "gzip"
    else 
        contentEncoding = ""
    end 
    #arguments = Dict("Bucket"   => h.bkt,
    #             "Key"      => joinpath(h.keyPrefix, key),
   #              "Body"     => data, 
   #              "Content-Type"  => CONTENT_TYPE,
   #              "Content-Encoding" => contentEncoding)
    #@show arguments
    #resp = S3.put_object(AWS_CREDENTIAL, arguments)
    resp = s3_put(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key), data; 
                  metadata = Dict("Content-Type" => CONTENT_TYPE, 
                                  "Content-Encoding" => contentEncoding))
    nothing
end

function Base.getindex(h::S3Dict, key::AbstractString)
    try 
        #data = S3.get_object(AWS_CREDENTIAL; 
        #                     Bucket=h.bkt, 
        #                     Key=joinpath(h.keyPrefix, key))
        data = s3_get(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key))
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
    nothing
end

function Base.delete!( h::S3Dict, key::AbstractString)
    #S3.delete_object(AWS_CREDENTIAL; Bucket=h.bkt, Key=joinpath(h.keyPrefix, key))
    s3_delete(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key))
end

function Base.keys( h::S3Dict )
    #S3.list_objects_v2(AWS_CREDENTIAL; Bucket=h.bkt, prefix=h.keyPrefix)
    s3_list_objects(AWS_CREDENTIAL, h.bkt, h.keyPrefix)
end

function Base.values(h::S3Dict)
    error("normally values are too large to get them all to RAM")
end

function Base.haskey(h::S3Dict, key::String)
    #resp = S3.list_objects_v2(AWS_CREDENTIAL; Bucket=h.bkt, prefix=joinpath(h.keyPrefix, key))
    #return Meta.parse( resp["KeyCount"] ) > 0
    s3_exists(AWS_CREDENTIAL, h.bkt, joinpath(h.keyPrefix, key))
end 

end # end of module S3Dicts
