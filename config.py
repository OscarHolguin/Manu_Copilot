#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os

""" Bot Configuration """


class DefaultConfig:
    """ Bot Configuration """

    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "6cd09ea4-5df3-4425-8b6e-87f0d24b952f")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "MKp8Q~lLqrM6l.q0SIJODjtqzPaW_opIuBzCjaWF")
    # APP_ID = os.environ.get("MicrosoftAppId", "")
    # APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")

    