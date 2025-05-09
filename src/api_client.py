import logging, aiohttp, os, asyncio
import random

from src.models import Holding

logger = logging.getLogger(__name__)


class APIClient:
    def __init__(self):
        try:
            self.base_api_url = os.environ["BASE_API_URL"]
        except KeyError as e:
            logger.error("Environment variable %s not found", e)
            raise e

    async def fetch_holdings(self, filing_id: str, session: aiohttp.ClientSession):
        """
        Fetch holdings data with retries using exponential backoff and jitter.
        Keyword Arguments:
        filing_id: the id of the quarter that is to be fetched
        Returns Holding objects with 'COM' class for the filing provided
        """
        max_retries = 3
        base_delay = 1  # initial delay in seconds

        for attempt in range(max_retries + 1):
            url = self.base_api_url + filing_id
            try:
                async with session.get(url) as response:
                    if response.status == 500:
                        raise aiohttp.ClientResponseError(
                            status=response.status,
                            request_info=response.request_info,
                            history=response.history,
                            message=f"Server responded with status {response.status}",
                        )
                    response.raise_for_status()
                    try:
                        data_json = await response.json()
                    except Exception as json_err:
                        response_text = await response.text()
                        logger.error(
                            f"Failed to parse JSON for filing {filing_id}. Response text: {response_text}"
                        )
                        raise json_err

                    holdings_list = data_json.get("data", [])
                    holdings = []
                    # Process the holdings list: filter out records with errors
                    for record in holdings_list:
                        if record[0] is None or record[8] is not None:
                            continue
                        if record[2] == "COM":
                            holdings.append(
                                Holding(
                                    record[0],
                                    record[2],
                                    record[4],
                                    record[5],
                                    record[6],
                                )
                            )
                    return holdings

            except aiohttp.ClientResponseError as e:
                if e.status == 500:
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        # Add random jitter between 0 and 0.5 seconds
                        jitter = random.uniform(0, 0.5)
                        logger.warning(
                            f"Attempt {attempt + 1} for {url} failed with status 500; Retrying in {delay + jitter} seconds...",
                        )
                        await asyncio.sleep(delay + jitter)
                        continue
                    else:
                        logger.error(f"Max retries reached for {url}.")
                        raise e
                else:
                    raise e

            except Exception as e:
                if attempt < max_retries:
                    delay = base_delay * (2**attempt)
                    jitter = random.uniform(0, 0.5)
                    logger.warning(
                        f"Attempt {attempt + 1} for {url} failed with error {e}; Retrying in {delay + jitter} seconds..."
                    )
                    await asyncio.sleep(delay + jitter)
                    continue
                else:
                    logger.error(
                        f"Max retries reached for filing {url} after error {e}."
                    )
                    raise e
