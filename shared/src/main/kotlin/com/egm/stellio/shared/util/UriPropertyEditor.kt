package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.UriUtils.toUri
import java.beans.PropertyEditorSupport

class UriPropertyEditor : PropertyEditorSupport() {

    override fun setAsText(source: String) =
        setValue(source.toUri())
}
