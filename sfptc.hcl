# strategy git {}
# strategy docker {}
# strategy hermit {}
# strategy artifactory {
#   mitm = ["artifactory.square.com"]
# }

host "/github/" {
  target = "https://github.com/"
}

disk {
  root = "./cache"
}
