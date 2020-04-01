provider "nomad" {
  version = "~> 1.4"
}
variable image_repository {
  default = "vxlabs/nest"
}
variable image_tag {
    default = "latest"
}

resource "nomad_job" "nest" {
  jobspec = templatefile("${path.module}/template.nomad.hcl",
    {
      service_image        = var.image_repository,
      service_version        = var.image_tag,
    },
  )
}
