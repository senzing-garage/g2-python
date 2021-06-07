# Update individual repositories

1. Update CHANGELOG.md
   1. This can be done in a "batch fashion".
2. :pencil2: Commit repository

```console
git status
git commit -a -m "#3 2.3.0"
git push
git status
```

3. Create pull request with the following title:

```console
Shipped with SenzingAPI 2.3.0
```

4. Pull request, but do not delete branch
5. Create a version based on the artifact version, not SenzingAPI version

```console
See [CHANGELOG.md]()
```
