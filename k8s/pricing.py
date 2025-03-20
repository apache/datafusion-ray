import boto3
import json

region_mapping = {
    "ap-east-1": "Asia Pacific (Hong Kong)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka-Local)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ca-central-1": "Canada (Central)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "sa-east-1": "South America (Sao Paulo)",
    "us-east-1": "US East (N. Virginia)",
    "us-west-1": "US West (N. California)",
    "us-east-2": "US East (Ohio)",
    "us-west-2": "US West (Oregon)",
    "cn-north-1": "China (Beijing)",
    "cn-northwest-1": "China (Ningxia)",
    "us-gov-west-1": "AWS GovCloud (US)",
}


def get_on_demand_price(region, instance_type):
    """Get on-demand price for a specific instance type in a region."""
    pricing_client = boto3.client("pricing", region_name="us-east-1")

    try:
        response = pricing_client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {
                    "Type": "TERM_MATCH",
                    "Field": "location",
                    "Value": region_mapping[region],
                },
            ],
        )

        if response["PriceList"]:
            price_data = eval(response["PriceList"][0])
            terms = price_data["terms"]["OnDemand"]
            price_dimensions = next(iter(terms.values()))["priceDimensions"]
            price = next(iter(price_dimensions.values()))["pricePerUnit"]["USD"]
            return float(price)
    except Exception as e:
        print(
            f"Error getting on-demand price for {instance_type} in {region}: {str(e)}"
        )
    return None


def get_reserved(region, instance_type):
    """Get savings plans rates for a specific instance type in a region."""
    savingsplans_client = boto3.client("savingsplans", region_name="us-east-1")

    def request():
        results = []
        nt = None
        while True:
            kwargs = dict(
                filters=[
                    {"name": "instanceType", "values": [instance_type]},
                    {"name": "region", "values": [region]},
                    {"name": "tenancy", "values": ["shared"]},
                    {"name": "productDescription", "values": ["Linux/UNIX"]},
                ],
                serviceCodes=["AmazonEC2"],
            )
            if nt:
                kwargs["nextToken"] = nt

            response = savingsplans_client.describe_savings_plans_offering_rates(
                **kwargs
            )
            results.extend(response["searchResults"])
            if len(response["nextToken"]) > 0:
                nt = response["nextToken"]
            else:
                break
        return results

    try:
        results = request()
        rates = {
            "1.0y": {"no": None, "partial": None, "all": None},
            "3.0y": {"no": None, "partial": None, "all": None},
        }

        for result in results:
            # Skip unused box entries
            if "UnusedBox" in result["usageType"]:
                continue

            duration_seconds = result["savingsPlanOffering"]["durationSeconds"]
            duration_years = duration_seconds / (365 * 24 * 60 * 60)
            key = f"{duration_years:.1f}y"

            payment_option = (
                result["savingsPlanOffering"]["paymentOption"].lower().split()[0]
            )  # 'no', 'partial', or 'all'
            rate = float(result["rate"])

            if key in rates:
                rates[key][payment_option] = rate

        return (
            rates
            if any(any(v is not None for v in year.values()) for year in rates.values())
            else None
        )
    except Exception as e:
        print(f"Error getting reserved cost for {instance_type} in {region}: {str(e)}")
    return None
