# strategy git {}
# strategy docker {}
# strategy hermit {}
# strategy artifactory {
#   mitm = ["artifactory.global.square"]
# }

host "/github/" {
  target = "https://github.com/"
}

disk {
  root = "./cache"
}
