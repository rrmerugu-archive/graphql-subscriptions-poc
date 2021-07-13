``
**NOTE**: Using ariadne master not an official version for subscription functionality.  

## installation requirements
`pipenv install`

## GraphQL queries
```shell

subscription{
  message{
    to
    sender
    message
  }
}

mutation{
  send(sender: "Ravi", to: "Roja", message: "Hey baby")
}
```


## References
- https://github.com/mirumee/ariadne/issues/165
- https://www.gitmemory.com/issue/mirumee/ariadne-website/77/803012616