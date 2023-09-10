package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.UriPropertyEditor
import org.springframework.web.bind.WebDataBinder
import org.springframework.web.bind.annotation.InitBinder
import java.net.URI

open class BaseHandler {

    @InitBinder
    fun initBinder(binder: WebDataBinder) {
        binder.registerCustomEditor(
            URI::class.java,
            UriPropertyEditor()
        )
    }
}
