package org.fluffytiger.orientdbsink.messages

data class VkMessage(val id: String, val typeName: String, val className: String, val properties: Map<String, String>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VkMessage

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}
