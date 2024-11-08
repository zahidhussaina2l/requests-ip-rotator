import ipaddress
from random import choice, randint
import botocore.exceptions
import httpx
import asyncio
from aioboto3 import Session

MAX_IPV4 = ipaddress.IPv4Address._ALL_ONES

# Region lists that can be imported and used in the ApiGateway class
DEFAULT_REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1",
    "ca-central-1"
]

EXTRA_REGIONS = DEFAULT_REGIONS + [
    "ap-south-1", "ap-northeast-3", "ap-northeast-2",
    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
    "sa-east-1"
]

# These regions require manual opt-in from AWS
ALL_REGIONS = EXTRA_REGIONS + [
    "ap-east-1", "af-south-1", "eu-south-1", "me-south-1",
    "eu-north-1"
]


# Inherits from httpx.AsyncBaseTransport so that we can edit each request before sending
class ApiGateway(httpx.AsyncBaseTransport):

    def __init__(self, site, regions=DEFAULT_REGIONS, access_key_id=None, access_key_secret=None, verbose=True, **kwargs):
        super().__init__(**kwargs)
        # Set simple params from constructor
        if site.endswith("/"):
            self.site = site[:-1]
        else:
            self.site = site
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.api_name = site + " - IP Rotate API"
        self.regions = regions
        self.verbose = verbose
        self.endpoints = []
        self._client = httpx.AsyncClient()

    # Enter and exit blocks to allow "with" clause
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
        await self._client.aclose()

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        # Get random endpoint
        endpoint = choice(self.endpoints)
        
        # Parse the URL properly
        url = httpx.URL(str(request.url))
        
        # Get the path and query
        path = url.path or ""
        if path.startswith("/"):
            path = path[1:]
            
        # Create new URL with our endpoint
        new_url = f"https://{endpoint}/ProxyStage/{path}"
        if request.url.query:
            new_url += f"?{request.url.query.decode() if isinstance(request.url.query, bytes) else request.url.query}"
        
        # Create new headers dictionary, removing any existing host headers
        headers = {}
        for k, v in request.headers.items():
            if k.lower() != 'host':  # Skip any host headers
                headers[k] = v
                
        # Add single Host header
        headers["Host"] = endpoint
        
        # Handle X-Forwarded-For
        x_forwarded_for = ipaddress.IPv4Address._string_from_ip_int(randint(0, MAX_IPV4))
        headers["X-My-X-Forwarded-For"] = x_forwarded_for

        try:
            # Create and send new request
            new_request = httpx.Request(
                method=request.method,
                url=new_url,
                headers=headers,
                content=request.content,
            )
            
            if self.verbose:
                print(f"Making request to: {new_url}")
                print(f"Final headers: {dict(new_request.headers)}")
            
            return await self._client.send(new_request)
        except Exception as e:
            if self.verbose:
                print(f"Error in request: {str(e)}")
            raise

    async def aclose(self):
        await self._client.aclose()

    async def init_gateway(self, region, force=False, require_manual_deletion=False):
        # Init client
        session = Session()
        async with session.client(
            "apigateway",
            region_name=region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret
        ) as awsclient:
            # If API gateway already exists for host, return pre-existing endpoint
            if not force:
                try:
                    current_apis = await self.get_gateways(awsclient)
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "UnrecognizedClientException":
                        if self.verbose:
                            print(f"Could not create region (some regions require manual enabling): {region}")
                        return {
                            "success": False
                        }
                    else:
                        raise e

                for api in current_apis:
                    if "name" in api and api["name"].startswith(self.api_name):
                        return {
                            "success": True,
                            "endpoint": f"{api['id']}.execute-api.{region}.amazonaws.com",
                            "new": False
                        }

            # Create simple rest API resource
            new_api_name = self.api_name
            if require_manual_deletion:
                new_api_name += " (Manual Deletion Required)"
            create_api_response = await awsclient.create_rest_api(
                name=new_api_name,
                endpointConfiguration={
                    "types": [
                        "REGIONAL",
                    ]
                }
            )

            # Get ID for new resource
            get_resource_response = await awsclient.get_resources(
                restApiId=create_api_response["id"]
            )
            rest_api_id = create_api_response["id"]

            # Create "Resource" (wildcard proxy path)
            create_resource_response = await awsclient.create_resource(
                restApiId=create_api_response["id"],
                parentId=get_resource_response["items"][0]["id"],
                pathPart="{proxy+}"
            )

            # Allow all methods to new resource
            await awsclient.put_method(
                restApiId=create_api_response["id"],
                resourceId=get_resource_response["items"][0]["id"],
                httpMethod="ANY",
                authorizationType="NONE",
                requestParameters={
                    "method.request.path.proxy": True,
                    "method.request.header.X-My-X-Forwarded-For": True
                }
            )

            # Make new resource route traffic to new host
            await awsclient.put_integration(
                restApiId=create_api_response["id"],
                resourceId=get_resource_response["items"][0]["id"],
                type="HTTP_PROXY",
                httpMethod="ANY",
                integrationHttpMethod="ANY",
                uri=self.site,
                connectionType="INTERNET",
                requestParameters={
                    "integration.request.path.proxy": "method.request.path.proxy",
                    "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                }
            )

            await awsclient.put_method(
                restApiId=create_api_response["id"],
                resourceId=create_resource_response["id"],
                httpMethod="ANY",
                authorizationType="NONE",
                requestParameters={
                    "method.request.path.proxy": True,
                    "method.request.header.X-My-X-Forwarded-For": True
                }
            )

            await awsclient.put_integration(
                restApiId=create_api_response["id"],
                resourceId=create_resource_response["id"],
                type="HTTP_PROXY",
                httpMethod="ANY",
                integrationHttpMethod="ANY",
                uri=f"{self.site}/{{proxy}}",
                connectionType="INTERNET",
                requestParameters={
                    "integration.request.path.proxy": "method.request.path.proxy",
                    "integration.request.header.X-Forwarded-For": "method.request.header.X-My-X-Forwarded-For"
                }
            )

            # Creates deployment resource, so that our API to be callable
            await awsclient.create_deployment(
                restApiId=rest_api_id,
                stageName="ProxyStage"
            )

            # Return endpoint name and whether it show it is newly created
            return {
                "success": True,
                "endpoint": f"{rest_api_id}.execute-api.{region}.amazonaws.com",
                "new": True
            }

    @staticmethod
    async def get_gateways(client):
        gateways = []
        position = None
        complete = False
        
        while not complete:
            try:
                if isinstance(position, str):
                    gateways_response = await client.get_rest_apis(
                        limit=500,
                        position=position
                    )
                else:
                    gateways_response = await client.get_rest_apis(
                        limit=500
                    )

                gateways.extend(gateways_response['items'])
                position = gateways_response.get('position', None)
                if position is None:
                    complete = True
                    
            except botocore.exceptions.ClientError as e:
                # Handle errors
                raise e

        return gateways

    async def delete_gateway(self, region, endpoints=None):
        # Create client
        session = Session()
        deleted = []
        
        async with session.client(
            'apigateway',
            region_name=region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret
        ) as awsclient:
            # Extract endpoint IDs from given endpoints
            endpoint_ids = []
            if endpoints is not None:
                endpoint_ids = [endpoint.split(".")[0] for endpoint in endpoints]
            
            # Get all gateway apis
            try:
                apis = await self.get_gateways(awsclient)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "UnrecognizedClientException":
                    return []
                raise e

            # Delete APIs matching target name
            for api in apis:
                if "name" in api and self.api_name == api["name"]:
                    # Skip if endpoints specified and this one isn't in the list
                    if endpoints is not None and api["id"] not in endpoint_ids:
                        continue
                        
                    while True:
                        try:
                            await awsclient.delete_rest_api(restApiId=api["id"])
                            deleted.append(api["id"])
                            break
                        except botocore.exceptions.ClientError as e:
                            err_code = e.response["Error"]["Code"]
                            if err_code == "TooManyRequestsException":
                                await asyncio.sleep(1)
                                continue
                            else:
                                if self.verbose:
                                    print(f"Failed to delete API {api['id']}: {str(e)}")
                                break

        return deleted

    async def start(self, force=False, require_manual_deletion=False, endpoints=[]):
        # If endpoints given already, assign and continue
        if len(endpoints) > 0:
            self.endpoints = endpoints
            return endpoints

        if self.verbose:
            print(f"Starting API gateway{'s' if len(self.regions) > 1 else ''} in {len(self.regions)} regions.")
        
        self.endpoints = []
        new_endpoints = 0

        # Create tasks for each region initialization
        tasks = []
        for region in self.regions:
            task = self.init_gateway(
                region=region,
                force=force,
                require_manual_deletion=require_manual_deletion
            )
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                if self.verbose:
                    print(f"Error initializing gateway: {str(result)}")
                continue
                
            if result["success"]:
                self.endpoints.append(result["endpoint"])
                if result["new"]:
                    new_endpoints += 1

        if self.verbose:
            print(f"Using {len(self.endpoints)} endpoints with name '{self.api_name}' ({new_endpoints} new).")
        return self.endpoints

    async def shutdown(self, endpoints=None):
        if self.verbose:
            print(f"Deleting gateway{'s' if len(self.regions) > 1 else ''} for site '{self.site}'.")
        
        # Create tasks for each region deletion
        tasks = []
        for region in self.regions:
            task = self.delete_gateway(region=region, endpoints=endpoints)
            tasks.append(task)

        # Wait for all deletions to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        deleted = []
        for result in results:
            if isinstance(result, Exception):
                if self.verbose:
                    print(f"Error during shutdown: {str(result)}")
                continue
            deleted.extend(result)

        if self.verbose:
            print(f"Deleted {len(deleted)} endpoints for site '{self.site}'.")
        return deleted
