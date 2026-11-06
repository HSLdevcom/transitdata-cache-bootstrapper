#!/bin/bash
# Needs a GitHub token with read:packages in GITHUB_TOKEN to resolve transitdata-common from GitHub Packages
docker build --secret id=github_token,env=GITHUB_TOKEN -t hsldevcom/transitdata-cache-bootstrapper .
