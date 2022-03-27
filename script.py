from datetime import datetime
import requests
from es_util import ElasticClient
from elasticsearch import helpers

es = ElasticClient()


def index_contents(contents, INDEX):
    try:
        actions = [
            {
                "_index": INDEX,
                "_source": content,
            } for content in contents
        ]
        helpers.bulk(es, actions)
        print(f"succesfully persist {len(contents)} docs")
    except Exception as e:
        print("Some hicups", e)


def get_metadata(url: str):
    try:
        import requests
        if "ipfs" in url:
            url = url.replace("ipfs://", "https://ipfs.io/ipfs/")
        url = url.replace(".json", "")
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print("Error", url)


def map_post(post: dict):
    post["profile"] = post.pop("profileId", {})
    metadata = get_metadata(post.get("contentURI", ""))
    if metadata and "content" in metadata:
        post["metadata"] = metadata
    post["createdAt"] = datetime.fromtimestamp(
        int(post.get("timestamp", 100000)))
    if post.get("comments", []):
        post["comments"] = [map_post(c) for c in post.get("comments", [])]
    profile = get_profile(hex(int(post["profile"].get("id"))))
    if profile:
        post["profile"] = profile
    return post


def map_post_2(post: dict):
    post["has_error"] = True
    post["profile"] = post.pop("profileId", {})
    metadata = {
        "description": None,
        "content": post.get("contentURI").strip("data:,").strip(" "),
        "external_url": None,
        "image": None,
        "name": "",
        "attributes": [],
        "media": [],
        "animation_url": None
    }
    post["metadata"] = metadata
    post["createdAt"] = datetime.fromtimestamp(
        int(post.get("timestamp", 100000)))
    profile = get_profile(hex(int(post["profile"].get("id"))))
    if profile:
        post["profile"] = profile
    return post


def get_last_doc(index):
    q = {
        "size": 1,
        "sort": {"createdAt": "desc"},
        "query": {
            "match_all": {}
        }
    }
    return es.search(index=index, body=q)["hits"]["hits"][0]["_source"]


BASE_URL = "https://api.thegraph.com/subgraphs/name/anudit/lens-protocol"


def get_posts(last_timestamp="0"):
    query = f"""
        {{
        posts(first: 1000, orderBy:timestamp, where: {{ timestamp_gte: \"{last_timestamp}\" }}) {{
            id
            pubId
            profileId {{
            id
            handle
            owner
            pubCount
            imageURI
            createdOn
            creator
            followNFTURI
            followNFT
            }}
            contentURI
            timestamp
            collectModule
            comments {{
            id
            contentURI
            timestamp
            profileId
            pubId
            }}
        }}
        }}
    """
    data = requests.post(BASE_URL, json={"query": query}).json()
    return data.get("data", {}).get("posts", [])


def get_profiles(last_created_on="0"):
    query = f"""
        {{
        profiles(first: 1000, orderBy:createdOn, where: {{ createdOn_gte: \"{last_created_on}\" }}) {{
            id
            creator
            owner
            profileId
            pubCount
            followNFT
            handle
            imageURI
            followNFTURI
            createdOn
            followModule
            followModuleReturnData
            dispatcher
            }}
        }}
        }}
    """
    data = requests.post(BASE_URL, json={"query": query})
    return data.json()


def get_profile(profile_id):
    query = f"""
        query Profiles {{
          profiles(request: {{ profileIds: [\"{profile_id}\"], limit: 1 }}) {{
            items {{
              id
              name
              bio
              location
              website
              twitterUrl
              picture {{
                ... on NftImage {{
                  contractAddress
                  tokenId
                  uri
                  verified
                }}
                ... on MediaSet {{
                  original {{
                    url
                    mimeType
                  }}
                }}
                __typename
              }}
              handle
              coverPicture {{
                ... on NftImage {{
                  contractAddress
                  tokenId
                  uri
                  verified
                }}
                ... on MediaSet {{
                  original {{
                    url
                    mimeType
                  }}
                }}
                __typename
              }}
              ownedBy
              depatcher {{
                address
                canUseRelay
              }}
              stats {{
                totalFollowers
                totalFollowing
                totalPosts
                totalComments
                totalMirrors
                totalPublications
                totalCollects
              }}
              followModule {{
                ... on FeeFollowModuleSettings {{
                  type
                  amount {{
                    asset {{
                      symbol
                      name
                      decimals
                      address
                    }}
                    value
                  }}
                  recipient
                }}
                __typename
              }}
            }}
            pageInfo {{
              prev
              next
              totalCount
            }}
          }}
        }}
    """
    data = requests.post(
        "https://api-mumbai.lens.dev/playground", json={"query": query}).json()
    profiles = data.get("data", {}).get("profiles", {}).get("items", [])
    return profiles[0] if profiles else {}


def index_profiles():
    last_profile = {}
    while True:
        last_timestamp = last_profile.get("createdOn", "0")
        profiles = get_profiles(last_timestamp)
        if last_profile:
            for i in range(len(profiles)):
                if last_profile.get("id") == profiles[i].get("id"):
                    profiles = profiles[i+1:]
                    break
        if not profiles:
            break
        last_profile = profiles[-1]
        index_contents(profiles, "lens-new-profiles-data")


def index_posts():
    last_post = get_last_doc(index="lens-final-posts-data")
    while True:
        last_timestamp = last_post.get("timestamp", "0")
        posts = get_posts(last_timestamp)
        if posts:
            for i in range(len(posts)):
                if last_post.get("id") == posts[i].get("id"):
                    posts = posts[i+1:]
                    break
        if not posts:
            break
        last_post = posts[-1]
        new_posts = []
        print(f"mapping {len(posts)} posts")
        for post in posts:
            content = post.get("contentURI", "")
            if content.startswith("http") or ("ipfs" in content):
                new_posts.append(map_post(post))
            elif content.startswith("data:,"):
                new_posts.append(map_post_2(post))
        print(f"indexing {len(new_posts)} posts")
        index_contents(new_posts, "lens-final-posts-data")
