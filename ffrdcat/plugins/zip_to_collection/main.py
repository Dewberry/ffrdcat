import sys
from zip_to_collection import main, plugin_params
from papipyplug import parse_input, print_results, plugin_logger


if __name__ == "__main__":
    # Start plugin logger
    plugin_logger()

    # Read, parse, and verify input parameters
    input_params = parse_input(sys.argv, plugin_params)

    # Add main function here
    results = main(input_params)

    # Print Results
    print_results(results)
