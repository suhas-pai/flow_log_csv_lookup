import csv
import socket
import sys

from enum import Enum


class FlowLogFieldIndex(Enum):
    Version = 0
    AccountId = 1
    InterfaceId = 2
    SourceIp = 3
    DestinationIp = 4
    SourcePort = 5
    DestinationPort = 6
    Protocol = 7
    Packets = 8
    Bytes = 9
    Start = 10
    End = 11
    Action = 12


def main():
    if len(sys.argv) != 4:
        print(f"Expected 3 arguments, got {len(sys.argv) - 1}")
        print(f"Usage: {sys.argv[0]} <csv file> <flow log file> <output file>")

        sys.exit(1)

    csvFilePath = sys.argv[1]
    flowLogFilePath = sys.argv[2]
    outputFilePath = sys.argv[3]

    prefix = "IPPROTO_"
    protocolNumberToName = {
        num: name.lower()[len(prefix) :]
        for name, num in vars(socket).items()
        if name.startswith(prefix)
    }

    portAndProtocolToTag = {}
    try:
        with open(csvFilePath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    portAndProtocolToTag.update(
                        {(row.get("dstport"), row.get("protocol")): row.get("tag")}
                    )
                except KeyError:
                    print(
                        f"CSV file at path '{csvFilePath}' doesn't have a 'dstport' and 'protocol' column"
                    )
                    sys.exit(1)
    except FileNotFoundError:
        print(f"CSV file not found at path: {csvFilePath}")
        sys.exit(1)
    except PermissionError:
        print(f"CSV file not readable at path: {csvFilePath}")
        sys.exit(1)
    except IsADirectoryError:
        print(f"CSV file at path '{csvFilePath}' is a directory")
        sys.exit(1)
    except csv.Error as e:
        print(
            f"Encountered csv error while reading CSV file at path '{csvFilePath}': {e}"
        )
        sys.exit(1)
    except Exception:
        print(
            f"Encountered internal error while reading CSV file at path '{csvFilePath}': {sys.exc_info()}"
        )
        sys.exit(1)

    tagToCount = {}
    portAndProtocolToCount = {}

    try:
        with open(flowLogFilePath, "r") as f:  # Flow Log File
            for line in f:
                parts = line.split()
                if len(parts) == 0:
                    continue

                try:
                    dstport = parts[FlowLogFieldIndex.SourcePort.value]
                    protocolNumber = int(parts[FlowLogFieldIndex.Protocol.value])
                except KeyError:
                    print(
                        f"Flow log file at path '{flowLogFilePath}' is not in the correct format"
                    )
                    sys.exit(1)

                protocolName = protocolNumberToName.get(protocolNumber)
                tag = portAndProtocolToTag.get((dstport, protocolName))

                if tag is None:
                    tag = "untagged"

                tagToCount[tag] = tagToCount.get(tag, 0) + 1
                portAndProtocolToCount[(dstport, protocolName)] = (
                    portAndProtocolToCount.get((dstport, protocolName), 0) + 1
                )
    except FileNotFoundError:
        print(f"Flow log file not found at path: {flowLogFilePath}")
        sys.exit(1)
    except PermissionError:
        print(f"Flow log file at path '{flowLogFilePath}' is not readable")
        sys.exit(1)
    except IsADirectoryError:
        print(f"Flow log file at path '{flowLogFilePath}' is a directory")
        sys.exit(1)
    except Exception:
        print(
            f"Encountered internal error while processing flow-log file at path '{flowLogFilePath}': {sys.exc_info()[0]}"
        )
        sys.exit(1)

    try:
        with open(outputFilePath, "w") as f:
            f.write("Tag,Count\n")
            for tag, count in tagToCount.items():
                f.write(f"{tag},{count}\n")

            f.write("\nPort,Protocol,Count\n")
            for portAndProtocol, count in portAndProtocolToCount.items():
                port = portAndProtocol[0]
                protocol = portAndProtocol[1]

                f.write(f"{port},{protocol},{count}\n")
    except FileNotFoundError:
        print(f"Output file not found at path: {outputFilePath}")
        sys.exit(1)
    except PermissionError:
        print(f"Output file at path '{outputFilePath}' is not writable")
        sys.exit(1)
    except IsADirectoryError:
        print(f"Output file at path '{outputFilePath}' is a directory")
        sys.exit(1)
    except Exception:
        print(
            f"Encountered internal error while writing to {outputFilePath}: {sys.exc_info()[0]}"
        )
        sys.exit(1)

    print(f"Output written to {outputFilePath}")


if __name__ == "__main__":
    main()
