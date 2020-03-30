
import dns.resolver
import logging
import time


class DnsResolver:

    def __init__(self, nameserver="100.64.0.10", timeout=120):
        self.nameserver = nameserver

        self.resolver = dns.resolver.Resolver(configure=False)
        self.resolver.nameservers = [self.nameserver]

    def resolve(self, domain):
        timeout = 0
        while timeout < self.timeout:
            try:
                logging.info(f"resolving {domain}")

                addresses = []
                for response in self.resolver.query(domain):
                    if response.rdtype == dns.rdatatype.A:
                        addresses.append(response.address)

                return addresses

            except Exception:
                pass

            timeout += 5
            time.sleep(5)

        raise Exception(f"domain {domain} is not resolvable by nameserver {self.nameserver}")
